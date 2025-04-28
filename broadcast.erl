#!/usr/bin/env escript

-define(PROMPT, "").
-define(FORMAT, "~s~n").

-record(state, {node_id=null, msgid=1, store=[], topology=#{}}).

main([]) -> 
  io:setopts(standard_io, [{binary, true}]),
  loop(#state{}).

loop(State) ->
  case io:get_line(?PROMPT) of
    Line when is_binary(Line) -> 
      Msg = json:decode(Line),
      {_Reply, NewState} = handle(Msg, State),
      %% MsgOut = json:encode(Reply),
      %% io:fwrite(?FORMAT, [MsgOut]),
      %% lists:foreach(fun(R) -> io:fwrite(?FORMAT, [R]) end, Rs),
      loop(NewState);
    _Eof -> ok
  end.


handle(Msg, NodeId) ->
  #{<<"src">>  := Src,
    <<"dest">> := Dest,
    <<"body">> := Body} = Msg,
  #{<<"type">> := Tag} = Body,
  handle(Tag, {Src, Dest, Body}, NodeId).

handle(<<"init">> = Tag, {Src, Dest, Body}, _State) ->
  #{<<"type">>     := Tag,
    <<"msg_id">>   := MsgId,
    <<"node_id">>  := NodeId,
    <<"node_ids">> := _NodeIds} = Body,

  reply(Src, Dest, #{
    <<"type">> => <<"init_ok">>,
    <<"in_reply_to">> => MsgId
  }, #state{node_id=NodeId});


handle(<<"broadcast">> = Tag, {Src, Dest, Body}, State) ->
  #{<<"type">>    := Tag,
    <<"msg_id">>  := MsgId,
    <<"message">> := Message} = Body,

  Store = [Message|State#state.store],
  NewState = State#state{store=Store},

  NodeId = State#state.node_id,
  Topology = State#state.topology,

  maybe 
   #{NodeId := Neighbours} ?= Topology,
   false ?= lists:any(fun(X) -> X == Message end, State#state.store),
   lists:foreach(fun(N) -> gossip(N, Message, State) end, Neighbours)
  end,

  reply(Src, Dest, #{
    <<"type">> => <<"broadcast_ok">>,
    <<"msg_id">> => erlang:unique_integer([monotonic, positive]), 
    <<"in_reply_to">> => MsgId
  }, NewState);

handle(<<"read">> = Tag, {Src, Dest, Body}, State) ->
  #{<<"type">> := Tag, <<"msg_id">> := MsgId} = Body,

  reply(Src, Dest, #{
    <<"type">> => <<"read_ok">>,
    <<"msg_id">> => erlang:unique_integer([monotonic, positive]), 
    <<"in_reply_to">> => MsgId,
    <<"messages">> => State#state.store
  }, State);

handle(<<"topology">> = Tag, {Src, Dest, Body}, State) ->
  #{<<"type">>    := Tag,
    <<"msg_id">>  := MsgId,
    <<"topology">> := Topology} = Body,

  NewState = State#state{topology=Topology},

  reply(Src, Dest, #{
    <<"type">> => <<"topology_ok">>,
    <<"msg_id">> => erlang:unique_integer([monotonic, positive]), 
    <<"in_reply_to">> => MsgId
  }, NewState);

handle(<<"broadcast_ok">>, _Msg, State) -> {noreply, State};
handle(_Tag, _Msg, State) -> {noreply, State}.


reply(Dest, Src, Body, State) when State#state.node_id =:= Src ->
  reply(Dest, Body, State).

reply(Dest, Body, State) -> 
  Reply = #{
    <<"dest">> => Dest, 
    <<"src">>  => State#state.node_id,
    <<"body">> => Body
  },
  io:fwrite(?FORMAT, [json:encode(Reply)]),
  {Reply, State}.

gossip(Dest, Message, State) ->
  Body = #{
    <<"type">>    => <<"broadcast">>,
    <<"msg_id">>  => erlang:unique_integer([monotonic, positive]), 
    <<"message">> => Message},
  reply(Dest, Body, State).

