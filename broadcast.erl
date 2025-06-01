#!/usr/bin/env -S escript -c
-module(broadcast).
-export([init/0, handle_rpc/1]).
-define(PROMPT, "").
-define(FORMAT, "~s~n").

-record(state, {
	node_id=null,
	store=[],
	topology=#{},
	callbacks=#{},
  timers=#{}}).

main([]) -> 
  io:setopts(standard_io, [{binary, true}]),
	spawn_link(?MODULE, init, []),
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
	register(server, self()),
	register(server_rpc, spawn_link(?MODULE, handle_rpc, [#{}])),
	server(#state{}).

server(State) ->
	receive
		{line, Line} ->
      Msg = json:decode(Line),
			{ok, NewState} = handle(Msg, State),
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


handle(Msg, State) ->
  #{<<"src">>  := Src,
    <<"dest">> := Dest,
    <<"body">> := Body} = Msg,

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


handle(~"broadcast" = Tag, {Src, Dest, Body}, State) ->
  #{<<"type">>    := Tag,
    <<"msg_id">>  := MsgId,
    <<"message">> := Message} = Body,

  reply(Src, Dest, #{
    <<"type">> => <<"broadcast_ok">>,
    <<"msg_id">> => erlang:unique_integer([monotonic, positive]), 
    <<"in_reply_to">> => MsgId
  }, State),

  NewState = case lists:member(Message, State#state.store) of
    true -> State;
    _ -> 
      gossip(Src, Message, State),
      Store = [Message|State#state.store],
      State#state{store=Store}
  end,

	{ok, NewState};


handle(~"read" = Tag, {Src, Dest, Body}, State) ->
  #{<<"type">> := Tag, <<"msg_id">> := MsgId} = Body,

  reply(Src, Dest, #{
    <<"type">>        => <<"read_ok">>,
    <<"msg_id">>      => erlang:unique_integer([monotonic, positive]), 
    <<"in_reply_to">> => MsgId,
    <<"messages">>    => State#state.store
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

handle(~"broadcast_ok", #{~"in_reply_to" := MsgId} = _Msg, State) ->
  server_rpc ! {ok, MsgId},
  {ok, State};

%% handle(~"broadcast_ok", Msg, State) ->
%%   Timers = State#state.timers,
%% 
%%   Unacked = maybe
%%     #{~"in_reply_to" := MsgId} ?= Msg,
%%     #{MsgId := TRef} ?= Timers, 
%%     {ok, MsgId, TRef}
%%   end,
%% 
%%   case Unacked of
%%     {ok, Ref, Id} ->
%%       timers:cancel(Ref),
%%       NewState = State#state{timers = maps:remove(Id, Timers)},
%%       {ok, NewState};
%%     _ ->
%%       {ok, State}
%%   end;


handle(_Tag, _Msg, State) -> {ok, State}.

handle_info({reply, Reply}, State) ->
	io:format(?FORMAT, [json:encode(Reply)]),
	{ok, State};

handle_info({rpc, Msg}, State) ->
	io:format(?FORMAT, [json:encode(Msg)]),
	{ok, State};

handle_info({rpc, Msg, Id, Time} = Info, #state{timers = Timers} = State) ->
	io:format(?FORMAT, [json:encode(Msg)]),
  {ok, TRef} = timer:send_after(Time, {info, Info}),
  NewState = State#state{timers = Timers#{Id => TRef}},
	{ok, NewState}.


gossip(Src, Message, State) ->
  NodeId = State#state.node_id,
  Topology = State#state.topology,
  Data = State#state.store,
  maybe 
		#{NodeId := Neighbours} ?= Topology,
		false ?= lists:any(fun(X) -> X == Message end, Data),
		lists:foreach(fun
			(N) when N =:= Src -> ok;
			(N) -> broadcast(N, Message, State)
		end, Neighbours)
  end.

reply(Dest, Src, Body, State) when State#state.node_id =:= Src ->
  reply(Dest, Body, State).

reply(Dest, Body, State) -> 
  Reply = #{
    <<"dest">> => Dest, 
    <<"src">>  => State#state.node_id,
    <<"body">> => Body
  },
	%% self() ! {info, {reply, Reply}},
  server_rpc ! {reply, Reply},
	{ok, State}.

broadcast(Dest, Message, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  Msg = broadcast_msg(MsgId, State#state.node_id, Dest, Message),
  server_rpc ! {rpc, Msg, MsgId, 1000}.

broadcast_msg(MsgId, Src, Dest, Message) ->
  #{<<"dest">> => Dest, 
    <<"src">>  => Src,
    <<"body">> => #{
      <<"type">>    => <<"broadcast">>,
      <<"msg_id">>  => MsgId,
      <<"message">> => Message}}.

handle_rpc(State) ->
  receive
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


