#!/usr/bin/env -S escript -c
-module(kafka).

-export([init/0, handle_rpc/1]).

-define(PROMPT, "").
-define(FORMAT, "~s~n").


-record(state, {
	node_id  = null :: 'null' | binary(),
	node_ids = []   :: [binary()],
  data     = #{}  :: #{Key :: binary() := {
    Offset :: non_neg_integer(),
    Commit :: non_neg_integer(),
    Msgs :: [{Index :: non_neg_integer(), any()}]}}
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

handle_rpc(State) ->
  receive
    {reply, Msg} ->
      io:format(?FORMAT, [json:encode(Msg)]),
      handle_rpc(State);
    _ ->
      handle_rpc(State)
  end.

init() ->
	register(server_rpc, spawn_link(?MODULE, handle_rpc, [#{}])),
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
    node_ids = NodeIds
  },

  reply(Src, Dest, #{
    <<"type">> => <<"init_ok">>,
    <<"in_reply_to">> => MsgId
  }, NewState);


handle_msg(~"send", {Src, Dest, Body}, #state{data=Data} = State) ->
  #{<<"key">> := K, <<"msg">> := Msg, <<"msg_id">> := MsgId} = Body,

  {Offset, Commit, Log} = maps:get(K, Data, {0, 0, []}),
  Index = Offset+1,
  NewData = Data#{K => {Index, Commit, [{Index,Msg}]++Log}},
  NewState = State#state{data=NewData},

  reply(Src, Dest, #{
    <<"type">> => <<"send_ok">>,
    <<"offset">> => Index,
    <<"in_reply_to">> => MsgId
  }, NewState);

handle_msg(~"poll", {Src, Dest, Body}, #state{data=Logs} = State) ->
  #{<<"offsets">> := Offsets, <<"msg_id">> := MsgId} = Body,

  Msgs = maps:fold(fun (K, From, AccIn) ->
    case Logs of 
      #{K := {_Offset, _Commit, Log}} ->
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

handle_msg(~"commit_offsets", {Src, Dest, Body}, #state{data=Data} = State) ->
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

handle_msg(~"list_committed_offsets", {Src, Dest, Body}, State) ->
  #{<<"keys">> := Ks, <<"msg_id">> := MsgId} = Body,

  Map = maps:with(Ks, State#state.data),
  Offsets = #{K => Commit || K := {_,Commit,_} <- Map},

  reply(Src, Dest, #{
    <<"type">> => <<"list_committed_offsets_ok">>,
    <<"offsets">> => Offsets,
    <<"in_reply_to">> => MsgId
  }, State);

handle_msg(_Tag, _Msg, State) -> {ok, State}.

handle_info(_Msg, _State) -> ok.

reply(Dest, Src, Body, State) when State#state.node_id =:= Src ->
  reply(Dest, Body, State).

reply(Dest, Body, State) -> 
  Reply = #{
    <<"dest">> => Dest, 
    <<"src">>  => State#state.node_id,
    <<"body">> => Body},
  server_rpc ! {reply, Reply},
	{ok, State}.

