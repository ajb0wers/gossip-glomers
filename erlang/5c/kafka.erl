#!/usr/bin/env -S escript -c
-module(kafka).

-export([init/1, handle_send/2, handle_poll/2, handle_commit/2, handle_list/2]).

-define(CRASH, 13).
-define(ABORT, 14).
-define(KEY_DOES_NOT_EXIST, 20).
-define(PRECONDITION_FAILED, 22).

-define(RPC_ERR_CODE(Code, Body), map_get(~"code", Body) == Code).
-define(RPC_KEY_DOES_NOT_EXIST(Body),
  ?RPC_ERR_CODE(?KEY_DOES_NOT_EXIST, Body)).
-define(RPC_PRECONDITION_FAILED(Body),
  ?RPC_ERR_CODE(?PRECONDITION_FAILED, Body)).

-record(state, {
  node_id   = null :: 'null' | binary(),
  node_ids  = []   :: [binary()],
  callbacks = #{} :: #{MsgId::non_neg_integer() := any()},
  data = #{} :: #{Key::binary() := {
                    Length::non_neg_integer(),
                    Commit::non_neg_integer(),
                    Msgs::[{Offset::non_neg_integer(), any()}]}}
}).

main([]) -> 
  io:setopts(standard_io, [{binary, true}]),
  ServerPid = spawn_link(?MODULE, init, [server]),
  RpcOutPid = spawn_link(?MODULE, init, [rpcout]),
  register(server, ServerPid),
  register(rpcout, RpcOutPid),
  loop(standard_io).

init(rpcout) -> rpcout();
init(server) -> server(fun handle_msg/2, #state{}).

loop(standard_io) -> 
  case io:get_line([]) of
    eof -> ok;
    {error, Reason} -> exit(Reason);
    Line ->
      server ! {line, Line},
      loop(standard_io)
  end.

rpcout() ->
  receive
    Msg ->
      Reply = json:encode(Msg),
      io:format("~s~n", [Reply]),
      rpcout()
  end.

server(Fn, State) ->
  receive 
    {line, Line} ->
      Msg = parse_line(Line),
      Reply = Fn(Msg, State),
      server_reply(Fn, Reply);
    Msg ->
      Reply = Fn(Msg, State),
      server_reply(Fn, Reply)
  end.

server_reply(Fn, {ok, State}) ->
  server(Fn, State);
server_reply(Fn, {reply, Reply, State}) ->
  rpcout ! Reply,
  server(Fn, State);
server_reply(Fn, {noreply, State, Info}) ->
  server_continue(Fn, Info, State);
server_reply(Fn, {reply, Reply0, State, Info}) ->
  rpcout ! Reply0,
  server_continue(Fn, Info, State);
server_reply(_Fn, stop) ->
  ok.

server_continue(Fn, Info, State) ->
  Reply = Fn(Info, State),
  server_reply(Fn, Reply).

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


handle_msg({~"send", _Src, _Dest, _Body} = Msg, State) ->
  handle_send(Msg, State);
handle_msg({~"poll", _Src, _Dest, _Body} = Msg, State) ->
  handle_poll(Msg, State);
handle_msg({~"commit_offsets", _Src, _Dest, _Body} = Msg, State) ->
  handle_commit(Msg, State);
handle_msg({~"list_committed_offsets", _, _, _} = Msg, State) ->
  handle_list(Msg, State);
handle_msg({_,_,_, #{~"in_reply_to" := ReplyId}} = Msg, State) ->
  #{ReplyId := {Function, Data}} = State#state.callbacks, 
  Callbacks0 = State#state.callbacks,
  Callbacks = maps:remove(ReplyId, Callbacks0),
  NewState = State#state{callbacks=Callbacks},
  ?MODULE:Function({Msg, Data}, NewState);
handle_msg({_Tag, _Src, _Dest}, State) -> {ok, State}.

handle_send({~"send", _, _, Body} = Send, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  #{<<"key">> := Key} = Body,
  Callbacks0 = State#state.callbacks,
  Callbacks = Callbacks0#{MsgId => {handle_send, Send}},
  NewState = State#state{callbacks=Callbacks},
  reply(~"lin-kv", #{
    <<"type">>   => <<"read">>,
    <<"key">>    => Key,
    <<"msg_id">> => MsgId
  }, NewState);
handle_send({{~"read_ok", ~"lin-kv", _, Body}, Send}, State) ->
  #{~"value" := Value} = Body,
  handle_send({cas, Value, Send}, State);
handle_send({{~"error", ~"lin-kv", _Dest, Body}, Send}, State)
  when ?RPC_KEY_DOES_NOT_EXIST(Body) ->
  %% handle link-kv rpc read error (20) when key doesn't exist.
  %% TODO: expect `in_reply_to=msg_id`.
  handle_send({cas, -1, Send}, State);
handle_send({cas, From, Send}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  {~"send", _, _, #{<<"key">> := Key}} = Send,
  N = 1, Offset = From + N,

  Callbacks0 = State#state.callbacks,
  Callbacks = Callbacks0#{MsgId => {handle_send, {{Key,From,Offset,N}, Send}}},
  NewState = State#state{callbacks=Callbacks},

  reply(~"lin-kv", #{
    <<"type">>   => <<"cas">>,
    <<"key">>    => Key,
    <<"from">>   => From,
    <<"to">>     => Offset,
    <<"msg_id">> => MsgId,
    <<"create_if_not_exists">> => true
  }, NewState);
handle_send({{~"cas_ok", ~"lin-kv", Dest, _Body}, Info}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  {{Key, _, Offset, _N}, Msg} = Info,
  {~"send", _,_, #{<<"msg">> := Value}} = Msg,

  Callbacks0 = State#state.callbacks,
  Callbacks = Callbacks0#{MsgId => {handle_send, {{Key,Value,Offset}, Msg}}},
  NewState = State#state{callbacks=Callbacks},

  reply(~"seq-kv", Dest, #{
    <<"type">> => <<"write">>,
    <<"key">> => [Key,Offset],
    <<"value">> => Value,
    <<"msg_id">> => MsgId
  }, NewState);
handle_send({{~"error", ~"lin-kv", _Dest, Body}, {_, Msg} = _Send}, State)
  when ?RPC_PRECONDITION_FAILED(Body) ->
  %% handle lin-kv rpc cas error (22) when from value doesn't match.
  %% TODO: expect `in_reply_to=msg_id`.
  handle_send(Msg, State);
handle_send({{~"write_ok", ~"seq-kv", _, _}, Info}, State) ->
  {{_, _, Offset}, Msg} = Info,
  {~"send", Src, Dest, #{<<"msg_id">> := MsgId}} = Msg,
  reply(Src, Dest, #{
    <<"type">> => <<"send_ok">>,
    <<"offset">> => Offset,
    <<"in_reply_to">> => MsgId
  }, State).

handle_poll({~"poll", _Src, _Dest, Body} = Msg, State) -> 
  #{<<"offsets">> := Offsets} = Body,
  PollInfo = {maps:to_list(Offsets), #{}, Msg},
  %% TODO: link-kv read to get the tail offset.
  handle_poll({seq_read, PollInfo}, State);
handle_poll({seq_read, {[Log|_], _, _Msg} = PollInfo}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  {Key, Offset} = Log,

  Callbacks0 = State#state.callbacks,
  Callbacks = Callbacks0#{MsgId => {handle_poll, PollInfo}},
  NewState = State#state{callbacks=Callbacks},

  reply(~"seq-kv", #{
    <<"type">>   => <<"read">>,
    <<"key">>    => [Key,Offset],
    <<"msg_id">> => MsgId
  }, NewState);
handle_poll({seq_read, {[], Msgs, Msg}}, State) ->
  {~"poll", Src, _Dest, #{<<"msg_id">> := MsgId}} = Msg,
  reply(Src, #{
    <<"type">> => <<"poll_ok">>,
    <<"msgs">> => Msgs,
    <<"in_reply_to">> => MsgId
  }, State);
handle_poll({{~"read_ok", ~"seq-kv", _Dest, Body}, PollInfo}, State) ->
  #{~"value" := Value} = Body,
  {Logs0, Data0, Msg} = PollInfo, 
  [{Key, Offset}|Ls]  = Logs0,
  List = maps:get(Key, Data0, []),
  Data = Data0#{Key => [[Offset, Value] | List]},
  Logs = [{Key,Offset+1}|Ls],
  handle_poll({seq_read, {Logs, Data, Msg}}, State);
handle_poll({{~"error", ~"seq-kv", _Dest, Body}, PollInfo}, State)
  when ?RPC_KEY_DOES_NOT_EXIST(Body) ->
  {[{Key,_}|Logs], Data0, Msg} = PollInfo, 
  %% TODO: case Data0 of {}; or maps:get_or_default = []
  Data = case Data0 of 
    #{Key := List} -> Data0#{Key => lists:reverse(List)};
    _ -> Data0
  end,
  %% #{Key := List}  = Data0,
  %% Data = Data0#{Key => lists:reverse(List)}, 
  handle_poll({seq_read, {Logs, Data, Msg}}, State).


handle_commit({~"commit_offsets", _Src, _Dest, Body} = Msg, State) ->
  #{<<"offsets">> := Offsets} = Body,
  Info = {maps:to_list(Offsets), Msg},
  handle_commit({lin_read, Info}, State);
handle_commit({lin_read, {[Log|_], _Msg} = Info}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  {Key, _Offset} = Log,

  Callbacks0 = State#state.callbacks,
  Callbacks = Callbacks0#{MsgId => {handle_commit, Info}},
  NewState = State#state{callbacks=Callbacks},

  reply(~"lin-kv", #{
    <<"type">>   => <<"read">>,
    <<"key">>    => [<<"commit_offset">>,Key],
    <<"msg_id">> => MsgId
  }, NewState);
handle_commit({lin_read, {[], Msg} = _Info}, State) ->
  {~"commit_offsets", Src, _Dest, #{<<"msg_id">> := MsgId}} = Msg,
  reply(Src, #{
    <<"type">> => <<"commit_offsets_ok">>, 
    <<"in_reply_to">> => MsgId
  }, State);
handle_commit({{~"read_ok", ~"lin-kv", _Dest, Body}, Info}, State) ->
  #{~"value" := Value} = Body,
  {[{_Key,Offset}|Logs], Msg} = Info, 
  if
    Offset >= Value -> handle_commit({cas, Value, Info}, State);
    true -> handle_commit({lin_read, {Logs, Msg}}, State)
  end;
handle_commit({{~"error", ~"lin-kv", _Dest, Body}, Info}, State)
  when ?RPC_KEY_DOES_NOT_EXIST(Body) ->
  %% handle link-kv rpc read error (20) when key doesn't exist.
  %% TODO: expect `in_reply_to=msg_id`.
  handle_commit({cas, -1, Info}, State);
handle_commit({cas, From, Info}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  {Logs, _Msg} = Info,
  [{Key, Offset}|_] = Logs,

  Callbacks0 = State#state.callbacks,
  Callbacks = Callbacks0#{MsgId => {handle_commit, Info}},
  NewState = State#state{callbacks=Callbacks},

  reply(~"lin-kv", #{
    <<"type">>   => <<"cas">>,
    <<"key">>    => [<<"commit_offset">>, Key],
    <<"from">>   => From,
    <<"to">>     => Offset,
    <<"msg_id">> => MsgId,
    <<"create_if_not_exists">> => true
  }, NewState);
handle_commit({{~"cas_ok", _Src, _Dest, _Body}, Info}, State) ->
  {[_|Logs], Msg}  = Info,
  handle_commit({lin_read, {Logs, Msg}}, State);
handle_commit({{~"error", ~"lin-kv", _Dest, Body}, Info}, State)
  when ?RPC_PRECONDITION_FAILED(Body) ->
  %% handle lin-kv rpc cas error (22) when from value doesn't match.
  handle_commit({lin_read, Info}, State).

handle_list({~"list_committed_offsets", _Src, _Dest, Body} = Msg, State) ->
  #{<<"keys">> := Keys} = Body,
  handle_list({read_offsets, {Keys, #{}, Msg}}, State);
handle_list({read_offsets, {[Key|_], _Offsets, _Msg} = Info}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 

  Callbacks0 = State#state.callbacks,
  Callbacks = Callbacks0#{MsgId => {handle_list, Info}},
  NewState = State#state{callbacks=Callbacks},

  reply(~"lin-kv", #{
    <<"type">>   => <<"read">>,
    <<"key">>    => [<<"commit_offset">>, Key],
    <<"msg_id">> => MsgId
  }, NewState);
handle_list({read_offsets, {[], Offsets, Msg} = _Info}, State) ->
  {~"list_committed_offsets", Src, _Dest, #{<<"msg_id">> := MsgId}} = Msg,
  reply(Src, #{
    <<"type">> => <<"list_committed_offsets_ok">>, 
    <<"offsets">> => Offsets,
    <<"in_reply_to">> => MsgId
  }, State);
handle_list({{~"read_ok", ~"lin-kv", _Dest, Body}, Info}, State) ->
  #{~"value" := Value} = Body,
  {[Key|Keys], Offsets, Msg} = Info, 
  NewInfo = {Keys, Offsets#{Key => Value}, Msg},
  handle_list({read_offsets, NewInfo}, State);
handle_list({{~"error", ~"lin-kv", _Dest, Body}, Info}, State)
  when ?RPC_KEY_DOES_NOT_EXIST(Body) ->
  {[Key|Keys], Offsets, Msg} = Info, 
  NewInfo = {Keys, Offsets#{Key => 0}, Msg},
  handle_list({read_offsets, NewInfo}, State).

reply(Dest, Src, Body, State) when State#state.node_id =:= Src ->
  reply(Dest, Body, State).

reply(Dest, Body, State) -> 
  Reply = #{
    <<"dest">> => Dest, 
    <<"src">>  => State#state.node_id,
    <<"body">> => Body},
  {reply, Reply, State}.

parse_line(Line) ->
  Msg = json:decode(Line),
  #{<<"src">> := Src,
    <<"dest">> := Dest,
    <<"body">> := Body} = Msg,
  #{<<"type">> := Type} = Body,
  {Type, Src, Dest, Body}.

%% Highest Random Weight
%% https://jola.dev/posts/highest-random-weight-in-elixir
owner(Key, [_H|_] = Nodes) ->
  Hashes = [{N, erlang:phash2({Key, N})} || N <:- Nodes],
  {Node, _Max} = lists:foldl(fun
    ({_, Hash0} = Elem, AccIn) ->
      case AccIn of
        {_,Hash} when Hash0 > Hash -> Elem;
        _ -> AccIn
      end;
  end, hd(Hashes), Hashes),
  Node.
