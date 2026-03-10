#!/usr/bin/env -S escript -c
-module(kafka).

-export([rpc_request/1, rpc_reply/1]).

-record(state, {
  node_id   = null :: 'null' | binary(),
  node_ids  = []   :: [binary()],
  % append_msg = #{} :: #{Key::binary() := Values::list()},
  append_id = #{} :: #{MsgId::non_neg_integer() := any()},
  data = #{} :: #{Key::binary() := {
                    Length::non_neg_integer(),
                    Commit::non_neg_integer(),
                    Msgs::[{Offset::non_neg_integer(), any()}]}}
}).

main([]) -> 
  io:setopts(standard_io, [{binary, true}]),
  loop(standard_io).

loop(standard_io) -> 
  register(rpc_request, spawn_link(?MODULE, rpc_request, [noargs])),
  register(rpc_reply, spawn_link(?MODULE, rpc_reply, [noargs])),
  rpc_loop().

rpc_loop() ->
  case io:get_line([]) of
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
      Reply = handle_line(Line, State),
      rpc_request(Reply);
    _Other ->
      rpc_request(State)
  end;
rpc_request({ok, State}) ->
  rpc_request(State);
rpc_request({reply, Reply, State}) ->
  rpc_reply ! {reply, Reply},
  rpc_request(State);
rpc_request({reply, Reply0, State, Info}) ->
  rpc_reply ! {reply, Reply0},
  Reply = handle_continue(Info, State),
  rpc_request(Reply).


rpc_reply(noargs) ->
  rpc_reply(#{});
rpc_reply(State) ->
  receive
    {reply, Msg} ->
      Reply = json:encode(Msg),
      io:format("~s~n", [Reply]),
      rpc_reply(State)
  end.

parse_line(Line) ->
  Msg = json:decode(Line),
  #{<<"src">> := Src,
    <<"dest">> := Dest,
    <<"body">> := Body} = Msg,
  #{<<"type">> := Type} = Body,
  {Type, Src, Dest, Body}.

handle_line(Line, State) -> 
  Msg = parse_line(Line),
  handle_msg(Msg, State).

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


handle_msg({~"send", Src, Dest, Body}, #state{data=Streams} = State) ->
  #{<<"key">> := K, <<"msg">> := Msg, <<"msg_id">> := MsgId} = Body,

  {Offset, Logs} = append(K, Msg, Streams),
  %% List = map:get(K, State#state.append_msg, []),
  %% Msgs = State#state.append_list#{K => List++Msg),
  %% NewState = State#state{data=Logs, append_msg=Msgs},
  NewState = State#state{data=Logs},

  {reply, Reply, NewState} = reply(Src, Dest, #{
    <<"type">> => <<"send_ok">>,
    <<"offset">> => Offset,
    <<"in_reply_to">> => MsgId
  }, NewState),

  {reply, Reply, NewState, {read, K}};

handle_msg({~"poll", Src, Dest, Body}, #state{data=Logs} = State) ->
  #{<<"offsets">> := Offsets, <<"msg_id">> := MsgId} = Body,

  Msgs = read(Offsets, Logs),

  reply(Src, Dest, #{
    <<"type">> => <<"poll_ok">>,
    <<"msgs">> => Msgs,
    <<"in_reply_to">> => MsgId
  }, State);

handle_msg({~"commit_offsets", Src, Dest, Body}, #state{data=Data} = State) ->
  #{<<"offsets">> := Offsets, <<"msg_id">> := MsgId} = Body,

  NewData = commit(Offsets, Data),
  NewState = State#state{data=NewData},

  reply(Src, Dest, #{
    <<"type">> => <<"commit_offsets_ok">>,
    <<"in_reply_to">> => MsgId
  }, NewState);

handle_msg({~"list_committed_offsets", Src, Dest, Body}, State) ->
  #{<<"keys">> := Ks, <<"msg_id">> := MsgId} = Body,

  Offsets = list(Ks, State#state.data),

  reply(Src, Dest, #{
    <<"type">> => <<"list_committed_offsets_ok">>,
    <<"offsets">> => Offsets,
    <<"in_reply_to">> => MsgId
  }, State);

handle_msg({~"read_ok", ~"lin-kv" = Src, Dest, Body}, State) ->
  #{~"value" := Value, ~"in_reply_to" := ReplyId} = Body,
  MsgId = erlang:unique_integer([monotonic, positive]), 
  #{ReplyId := Key} = State#state.append_id,
  N = 1, To = Value + N,

  Callbacks0 = State#state.append_id,
  Callbacks1 = maps:remove(ReplyId, Callbacks0),
  Callbacks = Callbacks1#{MsgId => {Key, Value, To, N}},

  reply(Src, Dest, #{
    <<"type">> => <<"cas">>,
    <<"key">> => Key,
    <<"from">> => Value,
    <<"to">> => To,
    <<"create_if_not_exists">> => true,
    <<"msg_id">> => MsgId
  }, State#state{append_id=Callbacks});
handle_msg({~"cas_ok", ~"lin-kv", Dest, Body}, State) ->
  #{<<"in_reply_to">> := ReplyId} = Body,
  MsgId = erlang:unique_integer([monotonic, positive]), 
  #{ReplyId := {Key, Value, To, _N}} = State#state.append_id,

  Callbacks0 = State#state.append_id,
  Callbacks1 = maps:remove(ReplyId, Callbacks0),
  Callbacks = Callbacks1#{MsgId => {Key, Value, To}},

  reply(~"seq-kv", Dest, #{
    <<"type">> => <<"write">>,
    <<"key">> => [Key,To],
    <<"value">> => Value,
    <<"msg_id">> => MsgId
  }, State#state{append_id=Callbacks});
handle_msg({~"write_ok", ~"seq-kv", _Dest, Body}, State) ->
  #{<<"in_reply_to">> := ReplyId} = Body,
  #{ReplyId := {_Key, _Value, _Offset}} = State#state.append_id,

  Callbacks0 = State#state.append_id,
  Callbacks = maps:remove(ReplyId, Callbacks0),

  {ok, State#state{append_id=Callbacks}};
handle_msg({~"error", ~"lin-kv", _Dest, _Body}, State) ->
  %% #{<<"code">> := 22, <<"in_reply_to">> := MsgId} = Body,
  {ok, State};

handle_msg({_Tag, _Src, _Dest}, State) -> {ok, State}.

handle_continue({read, Key} = _Info, #state{append_id=Callbacks} = State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  NewState = State#state{append_id=Callbacks#{MsgId => Key}},
  reply(<<"lin-kv">>, #{
    <<"type">> => <<"read">>,
    <<"key">> => Key,
    <<"msg_id">> => MsgId
  }, NewState);
handle_continue(_Info, State) -> {ok, State}.

reply(Dest, Src, Body, State) when State#state.node_id =:= Src ->
  reply(Dest, Body, State).

reply(Dest, Body, State) -> 
  Reply = #{
    <<"dest">> => Dest, 
    <<"src">>  => State#state.node_id,
    <<"body">> => Body},
  {reply, Reply, State}.


append(Key, Msg, Logs) ->
  CreateIfNotExists = {0,0,[]},
  {Length, Commit, List} = maps:get(Key, Logs, CreateIfNotExists),
  Offset = Length+1,
  Appended = Logs#{Key => {Offset, Commit, [{Offset, Msg}]++List}}, 
  {Offset, Appended}.

read(Offsets, Logs) ->
  maps:fold(fun (K, From, AccIn) ->
    case Logs of 
      #{K := {_Length, _Commit, Log}} ->
        %% Pred = fun({I,_}) -> I >= From andalso I > Commit end,
        Pred = fun({I,_}) -> I >= From end,
        List = lists:takewhile(Pred, Log),
        Queue = lists:reverse([[I,H] || {I,H} <- List]),
        AccIn#{K => Queue};
      _NoMatch -> AccIn
    end
  end, #{}, Offsets).

commit(Offsets, Logs) ->
  maps:fold(fun (K, End, AccIn) -> 
    case Logs of
      #{K := {Offset, Commit, Log}} when End > Commit ->
        AccIn#{K := {Offset, End, Log}};
      _ -> AccIn
    end
  end, Logs, Offsets).

list(Keys, Logs) ->
  Map = maps:with(Keys, Logs),
  #{K => Commit || K := {_,Commit,_} <- Map}.
