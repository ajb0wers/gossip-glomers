#!/usr/bin/env -S escript -c
-module(broadcast).
-export([init/0, handle_rpc/1]).
-define(PROMPT, "").
-define(FORMAT, "~s~n").

-record(state, {
	node_id=null,
	data=[],
  messages=[],
	topology=#{},
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
  timer:send_interval(100, server, {info, broadcast}),
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


handle(~"broadcast" = Tag, {Src, Dest, Body}, State) ->
  #{<<"type">>    := Tag,
    <<"msg_id">>  := MsgId,
    <<"message">> := Message} = Body,
   
  NewState = case Message of
    Message when is_list(Message) -> State;
    _ ->
      case lists:member(Message, State#state.data) of
        true ->
          State;
        _ -> 
          Data = [Message|State#state.data],
          Messages = State#state.messages,
          List = gossip1(Src, Message, State),
          NewState0 = State#state{data=Data, messages=List++Messages},
          gossip(Src, Message, NewState0),
          NewState0
      end
  end,

  reply(Src, Dest, #{
    <<"type">> => <<"broadcast_ok">>,
    <<"msg_id">> => erlang:unique_integer([monotonic, positive]), 
    <<"in_reply_to">> => MsgId
  }, NewState);

handle(~"read" = Tag, {Src, Dest, Body}, State) ->
  #{<<"type">> := Tag, <<"msg_id">> := MsgId} = Body,

  reply(Src, Dest, #{
    <<"type">>        => <<"read_ok">>,
    <<"msg_id">>      => erlang:unique_integer([monotonic, positive]), 
    <<"in_reply_to">> => MsgId,
    <<"messages">>    => State#state.data
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

handle(~"broadcast_ok", {_, _, Body}, State) ->
  #{~"in_reply_to" := MsgId} = Body,
  server_rpc ! {ok, MsgId},
  {ok, State};

handle(_Tag, _Msg, State) -> {ok, State}.

handle_info({reply, Reply}, State) ->
	io:format(?FORMAT, [json:encode(Reply)]),
	{ok, State};

handle_info({rpc, Msg}, State) ->
	io:format(?FORMAT, [json:encode(Msg)]),
	{ok, State};

handle_info({rpc, Msg, Id, Time} = Info, #state{timers=Timers} = State) ->
	io:format(?FORMAT, [json:encode(Msg)]),
  {ok, TRef} = timer:send_after(Time, {info, Info}),
  NewState = State#state{timers = Timers#{Id => TRef}},
	{ok, NewState};

handle_info(broadcast, #state{node_id=Src,messages=Messages} = State) ->
  case Messages of
    [] -> {ok, State};
    _ ->
      Map = maps:groups_from_list(fun ({Dest, _}) -> Dest end, Messages),
      maps:foreach(fun (Dest, Values) ->
        MsgId = erlang:unique_integer([monotonic, positive]), 
        Msg = broadcast_msg(MsgId, Src, Dest, [N || {_,N} <- Values]),
        server_rpc ! {rpc, Msg, MsgId, 1000}
      end, Map),
      {ok, State#state{messages=[]}}
  end.

gossip1(Src, Message, State) ->
  NodeId = State#state.node_id,
  Topology = State#state.topology,
  #{NodeId := Neighbours} = Topology,
  [{Dest, Message} || Dest <- Neighbours, Dest =/= Src].

gossip(Src, Message, State) ->
  NodeId = State#state.node_id,
  Topology = State#state.topology,
  maybe 
		#{NodeId := Neighbours} ?= Topology,
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
  server_rpc ! {reply, Reply},
	{ok, State}.

broadcast(Dest, Message, #state{node_id=Src} = _State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  %% NewState = State#{messages => [{Src, Dest, Message}|Messages]},
  Msg = broadcast_msg(MsgId, Src, Dest, Message),
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


