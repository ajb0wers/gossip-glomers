#!/usr/bin/env -S escript -c
-module(counter).

-export([init/0, handle_rpc/1]).

-define(PROMPT, "").
-define(FORMAT, "~s~n").

-record(state, {
	node_id  = null :: 'null' | integer(),
	node_ids = []   :: list(),
	data     = #{}  :: map()
}).

main([]) -> 
  io:setopts(standard_io, [{binary, true}]),
	register(server, spawn_link(?MODULE, init, [])),
  loop().

loop() ->
  case io:get_line(?PROMPT) of
    Line when is_binary(Line) -> 
			server ! {line, Line},
      loop();
    eof ->
      stop()
  end.

init() ->
	register(server_rpc, spawn_link(?MODULE, handle_rpc, [#{}])),
  timer:send_interval(5000, server, {info, broadcast}),
	server(#state{}).

server(State) ->
	receive
		{line, Line} ->
			{ok, NewState} = handle_line(Line, State),
			server(NewState);
		{info, Msg} ->
			{ok, NewState} = handle_info(Msg, State),
			server(NewState);
		{eof, From} ->
			self() ! {stop, From},
			server(State);
		{stop, From} ->
			From ! ok
	end.

stop() ->
	server ! {eof, self()},
	receive ok -> erlang:halt(0) end.

handle_line(Line, State) ->
  Msg = json:decode(Line),
  #{<<"src">> := Src, <<"dest">> := Dest, <<"body">> := Body} = Msg,
  #{<<"type">> := Callback} = Body,
  handle_msg(Callback, {Src, Dest, Body}, State).

handle_msg(~"init", {Src, Dest, Body}, _State) ->
  #{<<"msg_id">>   := MsgId,
    <<"node_id">>  := NodeId,
    <<"node_ids">> := NodeIds} = Body,

  NewState = #state{
    node_id  = NodeId,
    node_ids = NodeIds,
    data     = #{NodeId => 0}
  },

  reply(Src, Dest, #{
    <<"type">> => <<"init_ok">>,
    <<"in_reply_to">> => MsgId
  }, NewState);


handle_msg(~"add", {Src, Dest, Body}, State) ->
  #{<<"msg_id">> := MsgId, <<"delta">> := Delta} = Body,

  NodeId = State#state.node_id,
  Data = State#state.data,
  #{NodeId := N} = Data,
  NewState = State#state{data=Data#{NodeId := N+Delta}},

  reply(Src, Dest, #{
    <<"type">> => <<"add_ok">>,
    <<"in_reply_to">> => MsgId
  }, NewState);


handle_msg(~"read", {Src, Dest, Body}, State) ->
  #{<<"msg_id">> := MsgId} = Body,
  Sum = fun (_, V, Acc0) -> V + Acc0 end,
  Value = maps:fold(Sum, 0, State#state.data),

  reply(Src, Dest, #{
    <<"type">>        => <<"read_ok">>,
    <<"in_reply_to">> => MsgId,
    <<"value">>       => Value
  }, State);

handle_msg(~"broadcast", {_Src, _Dest, Body}, State) ->
  #{<<"message">> := Message} = Body,
  Data = merge(Message, State#state.data),
  {ok, State#state{data=Data}};

handle_msg(~"broadcast_ok", {_, _, _Body}, State) ->
  {ok, State};

handle_msg(_Tag, _Msg, State) -> {ok, State}.

handle_info(broadcast, State) ->
  replicate(State), {ok, State}.


merge(Message, Data) when is_map(Message) ->
  Combiner = fun(_K, K1, K2) -> max(K1, K2) end,
  maps:merge_with(Combiner, Data, Message).

replicate(#state{node_id=Src} = State) ->
  NodeIds = State#state.node_ids,
  Data = State#state.data,

  lists:foreach(fun
    (Dest) when Dest =/= Src ->
      MsgId =  erlang:unique_integer([monotonic, positive]), 
      broadcast_msg(MsgId, Src, Dest, Data);
    (_Src) -> ok
  end, NodeIds).

reply(Dest, Src, Body, State) when State#state.node_id =:= Src ->
  reply(Dest, Body, State).

reply(Dest, Body, State) -> 
  Reply = #{
    <<"dest">> => Dest, 
    <<"src">>  => State#state.node_id,
    <<"body">> => Body},
  server_rpc ! {reply, Reply},
	{ok, State}.

broadcast_msg(MsgId, Src, Dest, Message) ->
  Msg = #{
    <<"dest">> => Dest, 
    <<"src">>  => Src,
    <<"body">> => #{
      <<"type">>    => <<"broadcast">>,
      <<"msg_id">>  => MsgId,
      <<"message">> => Message}},
  server_rpc ! {rpc, Msg}.
 
handle_rpc(State) ->
  receive
    {rpc, Msg} ->
      io:format(?FORMAT, [json:encode(Msg)]),
      handle_rpc(State);
    {rpc, Msg, Id, Time} ->
      io:format(?FORMAT, [json:encode(Msg)]),
      {ok, TRef} = timer:send_after(Time, {rpc, Msg, Id, Time}),
      NewState = State#{Id => TRef},
      handle_rpc(NewState);
    {ok, Id} ->
      #{Id := Timer} = State,
      timer:cancel(Timer),
      NewState = maps:remove(Id, State),
      handle_rpc(NewState);
    {reply, Msg} ->
      io:format(?FORMAT, [json:encode(Msg)]),
      handle_rpc(State);
    _ ->
      handle_rpc(State)
  end.


