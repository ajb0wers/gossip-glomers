#!/usr/bin/env -S escript -c
-module(pn_counter).

-export([init/0, handle_rpc/1]).

-define(PROMPT, "").
-define(FORMAT, "~s~n").

-record(state, {
	node_id  = null :: 'null' | integer(),
	node_ids = []   :: list(),
	inc     = #{}   :: map(),
	dec     = #{}   :: map()
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
    inc     = #{NodeId => 0},
    dec     = #{NodeId => 0}
  },

  reply(Src, Dest, #{
    <<"type">> => <<"init_ok">>,
    <<"in_reply_to">> => MsgId
  }, NewState);


handle_msg(~"add", {Src, Dest, Body}, State) ->
  #{<<"msg_id">> := MsgId, <<"delta">> := Delta} = Body,

  NodeId = State#state.node_id,

  NewState = if 
    Delta > 0 ->
      Inc = State#state.inc, #{NodeId := P} = Inc,
      State#state{inc=Inc#{NodeId := P+Delta}};
    Delta < 0 ->
      Dec = State#state.dec, #{NodeId := N} = Dec,
      State#state{dec=Dec#{NodeId := N+abs(Delta)}};
    Delta == 0 -> State
  end,

  reply(Src, Dest, #{
    <<"type">> => <<"add_ok">>,
    <<"in_reply_to">> => MsgId
  }, NewState);


handle_msg(~"read", {Src, Dest, Body}, State) ->
  #{<<"msg_id">> := MsgId} = Body,
  Sum = fun (_, V, Acc0) -> V + Acc0 end,
  Inc = maps:fold(Sum, 0, State#state.inc),
  Dec = maps:fold(Sum, 0, State#state.dec),
  Value = Inc - Dec,
  reply(Src, Dest, #{
    <<"type">>        => <<"read_ok">>,
    <<"in_reply_to">> => MsgId,
    <<"value">>       => Value
  }, State);

handle_msg(~"broadcast", {_Src, _Dest, Body}, State) ->
  #{<<"message">> := Message} = Body,
  {Inc, Dec} = merge(Message, State),
  {ok, State#state{inc=Inc, dec=Dec}};

handle_msg(~"broadcast_ok", {_, _, _Body}, State) ->
  {ok, State};

handle_msg(_Tag, _Msg, State) -> {ok, State}.

handle_info(broadcast, State) ->
  replicate(State), {ok, State}.


merge(Message, #state{inc=IncIn, dec=DecIn}) when is_map(Message) ->
  Combiner = fun(_K, V1, V2) -> max(V1, V2) end,
  #{<<"inc">> := Inc0, <<"dec">> := Dec0} = Message, 
  Inc = maps:merge_with(Combiner, Inc0, IncIn),
  Dec = maps:merge_with(Combiner, Dec0, DecIn),
  {Inc, Dec}.

replicate(#state{node_id=Src} = State) ->
  NodeIds = State#state.node_ids,
  Message = #{
     <<"inc">> => State#state.inc,
     <<"dec">> => State#state.dec
   },

  lists:foreach(fun
    (Dest) when Dest =/= Src ->
      broadcast_msg(Src, Dest, Message);
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

broadcast_msg(Src, Dest, Message) ->
  Msg = #{
    <<"dest">> => Dest, 
    <<"src">>  => Src,
    <<"body">> => #{
      <<"type">>    => <<"broadcast">>,
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


