#!/usr/bin/env -S escript -c
-module(txn).
-moduledoc """
Challenge #6b: Totally-Available, Read Uncommitted Transactions
https://www.fly.io/dist-sys/6b/
""".

-export([handle_txn/2]).

main([]) ->
  io:setopts(standard_io, [{binary, true}]),
  register(rpcout, spawn_link(fun rpcout/0)),
  register(server, spawn_link(fun server/0)),
  loop(standard_io).

%%%%%%%%%%%%%%%%%%%%%%%
%%% Server Handlers %%%
%%%%%%%%%%%%%%%%%%%%%%%

-define(TIMEOUT, 0).
-define(NODE_NOT_FOUND, 1).
-define(NOT_SUPPORTED, 10).
-define(TEMPORARILY_UNAVAILABLE, 11).
-define(MALFORMED_REQUEST, 12).
-define(CRASH, 13).
-define(ABORT, 14).
-define(KEY_DOES_NOT_EXIST, 20).
-define(KEY_ALREADY_EXIST, 21).
-define(PRECONDITION_FAILED, 22).
-define(TXN_CONFLICT, 30).

-define(RPC_ERR(Code, Body), map_get(~"code", Body) == Code).
-define(KEY_DOES_NOT_EXIST(Body), ?RPC_ERR(?KEY_DOES_NOT_EXIST, Body)).
-define(PRECONDITION_FAILED(Body), ?RPC_ERR(?PRECONDITION_FAILED, Body)).
-define(TXN_CONFLICT(Body), ?RPC_ERR(?TXN_CONFLICT, Body)).

-type nodeid() :: binary().
-type msgid()  :: non_neg_integer().
-type key()    :: binary().
-type value()  :: non_neg_integer().
-record #state{
  id        = null :: 'null' | nodeid(),
  nodes     = []   :: [nodeid()],
  callbacks = #{}  :: #{msgid() := any()},
  data      = #{}  :: #{key() := value()}
}.

server() -> server(fun handle_msg/2, #state{}).

handle_msg(Line, State)  when is_binary(Line) ->
  {noreply, State, parse_line(Line)};

handle_msg({init, Src, _Dest, Body}, State) ->
  #{
    <<"msg_id">>   := MsgId,
    <<"node_id">>  := NodeId,
    <<"node_ids">> := NodeIds
  } = Body,
  NewState = State#state{id = NodeId, nodes = NodeIds},
  reply(Src, #{
    <<"type">> => <<"init_ok">>,
    <<"in_reply_to">> => MsgId
  }, NewState);
handle_msg({txn, _, _, _} = Msg, State) ->
  handle_txn(Msg, State);
handle_msg({_, _, _, #{~"in_reply_to" := ReplyId}} = Msg, State) ->
  #{ReplyId := {Function, Data}} = State#state.callbacks,
  Callbacks0 = State#state.callbacks,
  Callbacks = maps:remove(ReplyId, Callbacks0),
  NewState = State#state{callbacks = Callbacks},
  erlang:apply(?MODULE, Function, [{Msg, Data}, NewState]);
handle_msg({rpc, Request, {_Function, _Data} = Info}, State) ->
  #{<<"body">> := #{<<"msg_id">> := MsgId}} = Request,
  Callbacks0 = State#state.callbacks,
  Callbacks = Callbacks0#{MsgId => Info},
  NewState = State#state{callbacks = Callbacks},
  {reply, Request, NewState};
handle_msg({_Tag, _Src, _Dest}, State) -> {ok, State}.

handle_txn({txn, _, _, _} = Msg, State) ->
  %% Info :: {Root::binary(), To::binary, Data::#{}, Msg} 
  Info = {_Root=~"", _To=~"", _Data=#{}, Msg},
  MsgId = erlang:unique_integer([monotonic, positive]),
  reply(~"lin-kv", #{
    ~"type" => ~"read",
    ~"key"  => ~"root",
    ~"msg_id" => MsgId
  }, State, _EventData = {handle_txn, Info});
handle_txn({{read_ok, ~"lin-kv", _Dest, Body}, {~"", To, Data, Msg}}, State) ->
  #{~"value" := Root} = Body,
  Info = {Root, To, Data, Msg},
  MsgId = erlang:unique_integer([monotonic, positive]),
  reply(~"lin-kv", #{
    ~"type" => ~"read",
    ~"key"  => [~"root", Root],
    ~"msg_id" => MsgId
  }, State, _EventData = {handle_txn, Info});
handle_txn({{error, _Src, _Dest, Body}, {~"", _, _, _} = Info}, State)
      when ?KEY_DOES_NOT_EXIST(Body) ->
  %% when root KEY_DOES_NOT_EXISTS(22) default data=#{}
  handle_txn({transact, Info}, State);
handle_txn({{read_ok, _Src, _Dest, Body}, {Root, To, _, Msg}}, State) ->
  #{~"value" := Data} = Body,
  Info = {Root, To, Data, Msg},
  handle_txn({transact, Info}, State);
handle_txn({transact, {Root, _, Data0, Msg}}, State) ->
  %% type=>write, key=>root:uuid, value=>transact(Ops, Data)
  {txn, _, _, #{~"txn" := Ops}} = Msg,
  {Txn, Data} = transact(Ops, Data0),
  To = uuid(),
  Info = {Root, To, Data, Txn, Msg},
  MsgId = erlang:unique_integer([monotonic, positive]),
  reply(~"lin-kv", #{
    ~"type" => ~"write",
    ~"key"  => [~"root", To],
    ~"value" => Data,
    ~"msg_id" => MsgId
  }, State, _EventData = {handle_txn, Info});
handle_txn({{write_ok, _, _, _}, {From, To, _, _, _} = Info}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]),
  reply(~"lin-kv", #{
    ~"type"   => ~"cas",
    ~"key"    => ~"root",
    ~"from"   => From,
    ~"to"     => To,
    ~"msg_id" => MsgId,
    ~"create_if_not_exists" => true
  }, State, _EventData = {handle_txn, Info});
handle_txn({{cas_ok, _, _, _}, {_, _, _, Txn, Msg}}, State) ->
  {txn, Src, _, #{<<"msg_id">> := MsgId}} = Msg,
  reply(Src, #{
    <<"type">>        => ~"txn_ok",
    <<"in_reply_to">> => MsgId,
    <<"txn">>         => lists:reverse(Txn)
  }, State);
handle_txn({{error, _, _, Body}, {_, _, _, _, Msg}}, State)
      when ?PRECONDITION_FAILED(Body) ->
  Backoff = ran:uniform(50),
  {ok, _} = erlang:send_after(Backoff, Msg),
  {ok, State};
handle_txn(_, State) ->
  {ok, State}.

transact(Ops, Data0) -> 
  lists:foldl(fun
    ([~"r", K, null], {List, Data}) ->
      V = maps:get(K, Data, null),
      {[[~"r", K, V] | List], Data};
    ([~"w", K, V] = W, {List, Data}) ->
      {[W | List], Data#{K => V}}
  end, {[], Data0}, Ops).

%%%%%%%%%%%%%$%%%%
%%% Server I/O %%%
%%%%%%%%%%%%%%%%%%

loop(standard_io) ->
  case io:get_line([]) of
    eof -> ok;
    Line ->
      server ! {rpc, Line},
      loop(standard_io)
  end.

rpcout() ->
  receive
    {rpc, Msg} ->
      Reply = json:encode(Msg),
      io:fwrite("~s~n", [Reply]),
      rpcout()
  end.

%%%%%%%%%%%%%%%%%%%%%%%
%%% Server Protocol %%%
%%%%%%%%%%%%%%%%%%%%%%%

server(Fn, State) ->
  receive
    {rpc, Msg} -> server_call(Fn, Msg, State)
  end.

server_call(Fn, Request, State) ->
  Reply = Fn(Request, State),
  server_reply(Fn, Reply).

server_reply(Fn, {ok, State}) ->
  server(Fn, State);
server_reply(Fn, {reply, Reply, State}) ->
  rpcout ! {rpc, Reply},
  server(Fn, State);
server_reply(Fn, {noreply, State, Info}) ->
  server_call(Fn, Info, State);
server_reply(Fn, {reply, Reply0, State, Info}) ->
  rpcout ! {rpc, Reply0},
  server_call(Fn, Info, State);
server_reply(_Fn, stop) ->
  ok.

reply(Dest, Body, #state{} = State) ->
  Reply = #{
    <<"src">>  => State#_.id,
    <<"dest">> => Dest,
    <<"body">> => Body},
  {reply, Reply, State}.

reply(Dest, Body, State, {_Fun, _Info} = EventData) ->
  Request = #{
    <<"src">>  => State#_.id,
    <<"dest">> => Dest,
    <<"body">> => Body},
  {noreply, State, {rpc, Request, EventData}}.

parse_line(Line) ->
  Msg = json:decode(Line),
  #{<<"src">> := Src,
    <<"dest">> := Dest,
    <<"body">> := Body} = Msg,
  #{<<"type">> := Type} = Body,
  {binary_to_existing_atom(Type), Src, Dest, Body}.

uuid() ->
  TsMs = os:system_time(millisecond),
  Rand = crypto:strong_rand_bytes(10),
  Uuid = generate(TsMs, Rand),
  binary:encode_hex(Uuid).

generate(UnixTsMs, <<RandA:12, RandB:62, _:6>>) ->
    Ver = 2#0111, Var = 2#10,
    <<UnixTsMs:48, Ver:4, RandA:12, Var:2, RandB:62>>.

