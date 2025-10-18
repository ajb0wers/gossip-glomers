#!/usr/bin/env -S escript -c
-module(kafka).

-export([rpc_request/1, rpc_reply/1]).

-define(PROMPT, "").
-define(FORMAT, "~s~n").


-record(state, {
	node_id  = null :: 'null' | binary(),
	node_ids = []   :: [binary()],
  data     = #{}  :: #{Key::binary() := {
    Length::non_neg_integer(),
    Commit::non_neg_integer(),
    Msgs :: [{Offset::non_neg_integer(), any()}]}}
}).

main([]) -> 
  io:setopts(standard_io, [{binary, true}]),
  loop(standard_io).

loop(standard_io) -> 
  register(rpc_request, spawn_link(?MODULE, rpc_request, [noargs])),
  register(rpc_reply, spawn_link(?MODULE, rpc_reply, [noargs])),
  rpc_loop().

rpc_loop() ->
  case io:get_line(?PROMPT) of
    eof -> ok;
    {error, Reason} -> exit(Reason);
    Line ->
      rpc_request ! {line, Line},
      rpc_loop()
  end.

rpc_request(noargs) ->
  rpc_request(#state{});
rpc_request(#state{} = State) ->
  receive 
    {line, Line} ->
      {ok, NewState} = handle_line(Line, State),
      rpc_request(NewState);
    _Other ->
      rpc_request(State)
  end.

rpc_reply(noargs) ->
  rpc_reply(#{});
rpc_reply(State) ->
  receive
    {reply, Msg} ->
      io:format(?FORMAT, [json:encode(Msg)]),
      rpc_reply(State)
  end.

handle_line(Line, State) -> 
  Msg = json:decode(Line),
  #{<<"src">> := Src, <<"dest">> := Dest, <<"body">> := Body} = Msg,
  #{<<"type">> := Type} = Body,
  handle_msg({Type, Src, Dest, Body}, State).

handle_msg({~"init", Src, Dest, Body}, State) ->
  #{<<"msg_id">>   := MsgId,
    <<"node_id">>  := NodeId,
    <<"node_ids">> := NodeIds} = Body,

  NewState = State#state{
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

reply(Dest, Src, Body, State) when State#state.node_id =:= Src ->
  reply(Dest, Body, State).

reply(Dest, Body, State) -> 
  Reply = #{
    <<"dest">> => Dest, 
    <<"src">>  => State#state.node_id,
    <<"body">> => Body},
  rpc_reply ! {reply, Reply},
	{ok, State}.

