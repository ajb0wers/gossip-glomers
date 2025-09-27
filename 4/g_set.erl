#!/usr/bin/env -S escript -c
-module(g_set).

-export([init/0, handle_rpc/1]).

-define(PROMPT, "").
-define(FORMAT, "~s~n").

-record(state, {
	node_id=null,
	data=sets:new(),
  messages=[],
	topology=#{},
  timers=#{}}).

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

stop() ->
	server ! {eof, self()},
	receive ok -> erlang:halt(0) end.

init() ->
	register(server_rpc, spawn_link(?MODULE, handle_rpc, [#{}])),
  timer:send_interval(5000, server, {info, broadcast}),
	server(#state{}).

server(State) ->
	receive
		{line, Line} ->
      Msg = json:decode(Line),
			{ok, NewState} = handle_line(Msg, State),
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


handle_line(Msg, State) ->
  #{<<"src">> := Src, <<"dest">> := Dest, <<"body">> := Body} = Msg,
  #{<<"type">> := Tag} = Body,
  handle(Tag, {Src, Dest, Body}, State).

handle(~"init" = Tag, {Src, Dest, Body}, _State) ->
  #{<<"type">>     := Tag,
    <<"msg_id">>   := MsgId,
    <<"node_id">>  := NodeId,
    <<"node_ids">> := _NodeIds} = Body,

  reply(Src, Dest, #{
    <<"type">> => <<"init_ok">>,
    <<"in_reply_to">> => MsgId
  }, #state{node_id=NodeId});


handle(~"add", {Src, Dest, Body}, State) ->
  #{<<"msg_id">> := MsgId, <<"element">> := Element} = Body,
  Data = sets:add_element(Element, State#state.data),
  NewState = State#state{data=Data},
  reply(Src, Dest, #{
    <<"type">> => <<"add_ok">>,
    <<"in_reply_to">> => MsgId
  }, NewState);


handle(~"read" = Tag, {Src, Dest, Body}, State) ->
  #{<<"type">> := Tag, <<"msg_id">> := MsgId} = Body,

  reply(Src, Dest, #{
    <<"type">>        => <<"read_ok">>,
    <<"in_reply_to">> => MsgId,
    <<"value">>       => sets:to_list(State#state.data)
  }, State);

handle(~"topology" = Tag, {Src, Dest, Body}, State) ->
  #{<<"type">>     := Tag,
    <<"msg_id">>   := MsgId,
    <<"topology">> := Topology} = Body,

  NewState = State#state{topology=Topology},

  reply(Src, Dest, #{
    <<"type">> => <<"topology_ok">>,
    <<"msg_id">> => erlang:unique_integer([monotonic, positive]), 
    <<"in_reply_to">> => MsgId
  }, NewState);

handle(~"broadcast", {_Src, _Dest, Body}, State) ->
  #{<<"message">> := Message} = Body,
  %% TODO: reply broadcast_ok
  handle_broadcast(Message, State);


handle(~"broadcast_ok", {_, _, _Body}, State) ->
  %% #{~"in_reply_to" := MsgId} = Body,
  %% server_rpc ! {ok, MsgId},
  {ok, State};

handle(_Tag, _Msg, State) -> {ok, State}.

handle_info(broadcast, State) -> replicate(State).


handle_broadcast(List, State) when is_list(List) ->
  Data = sets:union(State#state.data, sets:from_list(List)),
  {ok, State#state{data=Data}};
handle_broadcast(Message, State) ->
  Data = sets:add_element(Message, State#state.data),
  {ok, State#state{data=Data}}.

replicate(#state{node_id=Src} = State) ->
  Topology = State#state.topology,
  Data = sets:to_list(State#state.data),
  #{Src := Neighbours} = Topology,

  lists:foreach(fun
    (Dest) when Dest =/= Src ->
      Msg = broadcast_msg(Src, Dest, Data),
      server_rpc ! {rpc, Msg};
    (_Src) -> ok
  end, Neighbours),

  {ok, State}.

%% gossip1(Src, Message, State) ->
%%   NodeId = State#state.node_id,
%%   Topology = State#state.topology,
%%   #{NodeId := Neighbours} = Topology,
%%   [{Dest, Message} || Dest <- Neighbours, Dest =/= Src].

reply(Dest, Src, Body, State) when State#state.node_id =:= Src ->
  reply(Dest, Body, State).

reply(Dest, Body, State) -> 
  Reply = #{
    <<"dest">> => Dest, 
    <<"src">>  => State#state.node_id,
    <<"body">> => Body
  },
  server_rpc ! {reply, Reply},
	{ok, State}.

%% broadcast(Dest, Message, #state{node_id=Src} = _State) ->
%%   MsgId = erlang:unique_integer([monotonic, positive]), 
%%   Msg = broadcast_msg(MsgId, Src, Dest, Message),
%%   server_rpc ! {rpc, Msg, MsgId, 1000}.

broadcast_msg(Src, Dest, Message) ->
  #{<<"dest">> => Dest, 
    <<"src">>  => Src,
    <<"body">> => #{
      <<"type">>    => <<"broadcast">>,
      <<"message">> => Message}}.

%% broadcast_msg(MsgId, Src, Dest, Message) ->
%%   #{<<"dest">> => Dest, 
%%     <<"src">>  => Src,
%%     <<"body">> => #{
%%       <<"type">>    => <<"broadcast">>,
%%       <<"msg_id">>  => MsgId,
%%       <<"message">> => Message}}.
 
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


