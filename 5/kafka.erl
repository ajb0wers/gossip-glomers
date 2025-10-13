#!/usr/bin/env -S escript -c
-module(kafka).

-export([init/1]).

-define(PROMPT, "").
-define(FORMAT, "~s~n").


-record(state, {
	node_id  = null :: 'null' | binary(),
	node_ids = []   :: [binary()],
  data     = #{}  :: #{Key :: binary() := {
    Length :: non_neg_integer(),
    Commit :: non_neg_integer(),
    Msgs :: [{Offset :: non_neg_integer(), any()}]}}
}).

main([]) -> 
  io:setopts(standard_io, [{binary, true}]),
	register(server_rpc, self()),
  register(server, spawn_link(?MODULE, init, [noargs])),
  rpc_init(server).
  %%loop().

rpc_init(Pid) ->
  rpc_init(group_leader(), Pid).

rpc_init(IoDevice, Pid) ->
  Ref = erlang:monitor(process, IoDevice),
  rpc_request(#{ref=>Ref, device=>IoDevice, pid=>Pid}).

rpc_request(#{ref:=ReplyAs, device:=IoDevice} = State) ->
  Request = {get_line, unicode, ?PROMPT},
  IoDevice ! {io_request, self(), ReplyAs, Request},
  rpc_loop(State).

rpc_request(Line, #{pid:=Pid} = State) ->
  Pid ! {request, json:decode(Line)},
  rpc_request(State).

rpc_reply(Msg, #{device:=IoDevice} = State) ->
  M = io_lib, F = fwrite, A = [?FORMAT, [json:encode(Msg)]],
  Request = {put_chars, unicode, M, F, A},
  IoDevice !  {io_request, self(), IoDevice, Request},
  rpc_loop(State).

rpc_loop(#{ref:=ReplyAs, device:=IoDevice} = State) ->
  receive 
    {io_reply, ReplyAs, eof} -> stop();
    {io_reply, ReplyAs, {error, _}} -> stop();
    {io_reply, ReplyAs, Line} ->
      rpc_request(Line, State);
    {io_reply, IoDevice, ok} ->
      rpc_loop(State);
    {reply, Msg} ->
      rpc_reply(Msg, State);
    _Other ->
      rpc_loop(State)
  end.

init(noargs) ->
	server(#state{}).

server(State) ->
	receive
		{line, Line} ->
			{ok, NewState} = handle_line(Line, State),
			server(NewState);
		{request, Msg} ->
			{ok, NewState} = handle_request(Msg, State),
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

handle_line(Line, State) -> handle_request(json:decode(Line), State).

handle_request(Msg, State) ->
  #{<<"src">> := Src, <<"dest">> := Dest, <<"body">> := Body} = Msg,
  #{<<"type">> := Type} = Body,
  handle_msg({Type, Src, Dest, Body}, State).

handle_msg({~"init", Src, Dest, Body}, _State) ->
  #{<<"msg_id">>   := MsgId,
    <<"node_id">>  := NodeId,
    <<"node_ids">> := NodeIds} = Body,

  NewState = #state{
    node_id  = NodeId,
    node_ids = NodeIds
  },

  reply(Src, Dest, #{
    <<"type">> => <<"init_ok">>,
    <<"in_reply_to">> => MsgId
  }, NewState);


handle_msg({~"send", Src, Dest, Body}, #state{data=Data} = State) ->
  #{<<"key">> := K, <<"msg">> := Msg, <<"msg_id">> := MsgId} = Body,

  {Length, Commit, Log} = maps:get(K, Data, {0, 0, []}),
  Offset = Length+1,
  NewData = Data#{K => {Offset, Commit, [{Offset, Msg}]++Log}},
  NewState = State#state{data=NewData},

  reply(Src, Dest, #{
    <<"type">> => <<"send_ok">>,
    <<"offset">> => Offset,
    <<"in_reply_to">> => MsgId
  }, NewState);

handle_msg({~"poll", Src, Dest, Body}, #state{data=Logs} = State) ->
  #{<<"offsets">> := Offsets, <<"msg_id">> := MsgId} = Body,

  Msgs = maps:fold(fun (K, From, AccIn) ->
    case Logs of 
      #{K := {_Length, _Commit, Log}} ->
        %% Pred = fun({I,_}) -> I >= From andalso I > Commit end,
        Pred = fun({I,_}) -> I >= From end,
        List = lists:takewhile(Pred, Log),
        Queue = lists:reverse([[I,H] || {I,H} <- List]),
        AccIn#{K => Queue};
      _NoMatch -> AccIn
    end
  end, #{}, Offsets),

  reply(Src, Dest, #{
    <<"type">> => <<"poll_ok">>,
    <<"msgs">> => Msgs,
    <<"in_reply_to">> => MsgId
  }, State);

handle_msg({~"commit_offsets", Src, Dest, Body}, #state{data=Data} = State) ->
  #{<<"offsets">> := Offsets, <<"msg_id">> := MsgId} = Body,

  NewData = maps:fold(fun (K, End, AccIn) -> 
    case Data of
      #{K := {Offset, Commit, Log}} when End > Commit ->
        AccIn#{K := {Offset, End, Log}};
      _ -> AccIn
    end
  end, Data, Offsets),

  NewState = State#state{data=NewData},

  reply(Src, Dest, #{
    <<"type">> => <<"commit_offsets_ok">>,
    <<"in_reply_to">> => MsgId
  }, NewState);

handle_msg({~"list_committed_offsets", Src, Dest, Body}, State) ->
  #{<<"keys">> := Ks, <<"msg_id">> := MsgId} = Body,

  Map = maps:with(Ks, State#state.data),
  Offsets = #{K => Commit || K := {_,Commit,_} <- Map},

  reply(Src, Dest, #{
    <<"type">> => <<"list_committed_offsets_ok">>,
    <<"offsets">> => Offsets,
    <<"in_reply_to">> => MsgId
  }, State);

handle_msg({_Tag, _Src, _Dest}, State) -> {ok, State}.

handle_info(_Info, _State) -> ok.

reply(Dest, Src, Body, State) when State#state.node_id =:= Src ->
  reply(Dest, Body, State).

reply(Dest, Body, State) -> 
  Reply = #{
    <<"dest">> => Dest, 
    <<"src">>  => State#state.node_id,
    <<"body">> => Body},
  server_rpc ! {reply, Reply},
	{ok, State}.

