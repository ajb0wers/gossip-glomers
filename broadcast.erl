#!/usr/bin/env -S escript -c
-module(broadcast).

-export([print/0]).

-define(PROMPT, "").
-define(FORMAT, "~s~n").

-record(state, {node_id=null, msgid=1, store=[], topology=#{}}).

main([]) -> 
  io:setopts(standard_io, [{binary, true}]),
	register(printer, spawn(?MODULE, print, [])),
  loop(#state{}).

loop(State) ->
  case io:get_line(?PROMPT) of
    Line when is_binary(Line) -> 
      Msg = json:decode(Line),
      {_, NewState} = handle(Msg, State),
      loop(NewState);
    _Eof -> ok
  end.

print() ->
	receive 
		{msg, Msg} ->
			io:fwrite(?FORMAT, [json:encode(Msg)]),
			print();
		_ -> ok
	end.

	
handle(Msg, State) ->
  #{<<"src">>  := Src,
    <<"dest">> := Dest,
    <<"body">> := Body} = Msg,
  #{<<"type">> := Tag} = Body,
  handle(Tag, {Src, Dest, Body}, State).

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

  reply(Src, Dest, #{
    <<"type">> => <<"broadcast_ok">>,
    <<"msg_id">> => erlang:unique_integer([monotonic, positive]), 
    <<"in_reply_to">> => MsgId
  }, State),

 %% TODO: handle {continue, action}
	gossip(Src, Message, State),

  Store = [Message|State#state.store],
  NewState = State#state{store=Store},
	{noreply, NewState};


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

gossip(Src, Message, State) ->
  NodeId = State#state.node_id,
  Topology = State#state.topology,
  maybe 
		#{NodeId := Neighbours} ?= Topology,
		false ?= lists:any(fun(X) -> X == Message end, State#state.store),
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
	printer ! {msg, Reply},
	%% io:format(?FORMAT, [json:encode(Reply)]),
  {Reply, State}.

broadcast(Dest, Message, State) ->
  Body = #{
    <<"type">>    => <<"broadcast">>,
    <<"msg_id">>  => erlang:unique_integer([monotonic, positive]), 
    <<"message">> => Message},
  reply(Dest, Body, State).

