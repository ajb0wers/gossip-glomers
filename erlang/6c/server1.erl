#!/usr/bin/env -S escript -c
-module(txn).
-moduledoc """
Challenge #6c: Totally-Available, Read Committed Transactions
https://www.fly.io/dist-sys/6c/
""".
-export([handle_txn/2]).
-include("includes.hrl").

main([]) ->
  io:setopts(standard_io, [{binary, true}]),
  register(rpcout, spawn_link(fun rpcout/0)),
  register(server, spawn_link(fun server/0)),
  loop(standard_io).

%%%%%%%%%%%%%%%%%%%%%%%
%%% Server Handlers %%%
%%%%%%%%%%%%%%%%%%%%%%%

-type nodeid() :: binary().
-type msgid()  :: non_neg_integer().
-type key()    :: binary().
-type value()  :: non_neg_integer().

-record #state{
  id        = <<>> :: nodeid(),
  nodes     = []   :: [nodeid()],
  callbacks = #{}  :: #{msgid() := any()},
  data      = #{}  :: #{key() := value()}
}.

-record #data{
  id   = <<>> :: nodeid(),
  root = <<>>,
  to   = <<>>,
  txn  = [],
  data = #{},
  msg  = {}
}.

server() ->
  server(fun handle_msg/2, #state{}).

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
  Pid = spawn_link(fun txn/0),
  {noreply, State, call(Pid, Msg)};
handle_msg({_, _, _, #{~"in_reply_to" := ReplyId}} = Msg, State) ->
  #{ReplyId := Txn} = State#_.callbacks,
  Callbacks0 = State#_.callbacks,
  Callbacks = maps:remove(ReplyId, Callbacks0),
  NewState = State#_{callbacks = Callbacks},
  {noreply, NewState, call(Txn, Msg)};
handle_msg({rpc_request, From, Request}, State) ->
  #{<<"body">> := #{<<"msg_id">> := MsgId}} = Request,
  Callbacks0 = State#_.callbacks,
  Callbacks = Callbacks0#{MsgId => From},
  NewState = State#_{callbacks = Callbacks},
  {noreply, NewState, {rpc_reply, Request}};
handle_msg({rpc_reply, Reply}, State) ->
  rpcout ! {rpc, Reply},
  {noreply, State};
handle_msg({backoff, Msg}, State) ->
  Backoff = rand:uniform(50),
  Args = [self(), Msg],
  {ok, _} = timer:apply_after(Backoff, fun cast/2, Args),
  {noreply, State};
handle_msg(_, State) -> {noreply, State}.

txn() ->
  server(fun handle_txn/2, #data{}).

handle_txn({txn, _, Id, _} = Msg, #data{} = State) ->
  NewState = State#data{msg = Msg, id = Id},
  MsgId = erlang:unique_integer([monotonic, positive]),
  reply(~"lin-kv", #{
    ~"type" => ~"read",
    ~"key"  => ~"root",
    ~"msg_id" => MsgId
  }, NewState);
handle_txn({read_ok, ~"lin-kv", _Dest, Body}, #data{root = ~""} = State) ->
  #{~"value" := Root} = Body,
  NewState = State#data{root = Root},
  MsgId = erlang:unique_integer([monotonic, positive]),
  reply(~"lin-kv", #{
    ~"type" => ~"read",
    ~"key"  => [~"root", Root],
    ~"msg_id" => MsgId
  }, NewState);
handle_txn({error, _Src, _Dest, Body}, #data{root = ~""} = State)
      when ?KEY_DOES_NOT_EXIST(Body) ->
  %% when root KEY_DOES_NOT_EXISTS(22) default data=#{}
  handle_txn(transact, State);
handle_txn({read_ok, _Src, _Dest, Body}, State) ->
  #{~"value" := Data} = Body,
  NewState = State#data{data = Data},
  handle_txn(transact, NewState);
handle_txn(transact, State) ->
  Data0 = State#_.data,
  {txn, _, _, #{~"txn" := Ops}} = State#_.msg,
  {Txn, Data} = transact(Ops, Data0),
  To = uuid(),
  NewState = State#data{to = To, data = Data, txn = Txn},
  MsgId = erlang:unique_integer([monotonic, positive]),
  reply(~"lin-kv", #{
    ~"type" => ~"write",
    ~"key"  => [~"root", To],
    ~"value" => Data,
    ~"msg_id" => MsgId
  }, NewState);
handle_txn({write_ok, _, _, _}, #data{root = From, to = To} = State) ->
  MsgId = erlang:unique_integer([monotonic, positive]),
  reply(~"lin-kv", #{
    ~"type"   => ~"cas",
    ~"key"    => ~"root",
    ~"from"   => From,
    ~"to"     => To,
    ~"msg_id" => MsgId,
    ~"create_if_not_exists" => true
  }, State);
handle_txn({cas_ok, _, _, _}, #data{txn = Txn, msg = Msg} = State) ->
  {txn, Src, _, #{<<"msg_id">> := MsgId}} = Msg,
  ReplyMsg = message(State#_.id, Src, #{
    <<"type">>        => ~"txn_ok",
    <<"in_reply_to">> => MsgId,
    <<"txn">>         => lists:reverse(Txn)
  }),
  Reply = {rpc_reply, ReplyMsg},
  {reply, Reply, State, stop};
handle_txn({error, _, _, Body}, #data{msg = Msg} = State)
      when ?PRECONDITION_FAILED(Body) ->
  {reply, {backoff, Msg}, State, stop};
handle_txn(_Msg, State) ->
  {noreply, State}.

reply(Dest, Body, #state{} = State) ->
  Reply = message(State#_.id, Dest, Body),
  {noreply, State, {rpc_reply, Reply}};

reply(Dest, Body, #data{} = State) ->
  Request = message(State#_.id, Dest, Body),
  {reply, {rpc_request, self(), Request}, State}.

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
      cast(server, parse_line(Line)),
      loop(standard_io)
  end.

rpcout() ->
  receive
    {rpc, Msg} ->
      Reply = json:encode(Msg),
      io:fwrite("~s~n", [Reply]),
      rpcout()
  end.

parse_line(Line) ->
  Msg = json:decode(Line),
  #{<<"src">>  := Src,
    <<"dest">> := Dest,
    <<"body">> := Body} = Msg,
  #{<<"type">> := Type} = Body,
  {binary_to_existing_atom(Type), Src, Dest, Body}.

message(Src, Dest, Body) ->
  #{
    <<"src">>  => Src,
    <<"dest">> => Dest,
    <<"body">> => Body
  }.

%%%%%%%%%%%%%%%%%%%%%%%
%%% Server Protocol %%%
%%%%%%%%%%%%%%%%%%%%%%%

call(Server, Request) ->
  ReplyAs = make_ref(),
  Server ! {call, self(), ReplyAs, Request},
  receive {ReplyAs, Reply} ->
    Reply
  end.

cast(Server, Request) ->
  Server ! {cast, Request}, ok.

server(Fn, State) ->
  receive
    {cast, Msg} ->
      server_cast(Fn, Msg, State);
    {call, From, Ref, Msg} ->
      server_call(Fn, From, Ref, Msg, State)
  end.

server_cast(Fn, Request, State0) ->
  case Fn(Request, State0) of
    {noreply, State} ->
      server(Fn, State);
    {noreply, State, Info} ->
      server_continue(Fn, Info, State);
    stop ->
      ok
  end.

server_call(Fn, From, Ref, Request, State0) ->
  case Fn(Request, State0) of
    {reply, Reply, State} ->
      From ! {Ref, Reply},
      server(Fn, State);
    {reply, Reply0, State, Info} ->
      From !  {Ref, Reply0},
      server_continue(Fn, Info, State);
    {noreply, State, Info} ->
      server_continue(Fn, Info, State);
    stop ->
      ok
  end.

server_continue(Fn, Request, State) ->
  server_cast(Fn, Request, State).
uuid() ->
  TsMs = os:system_time(millisecond),
  Rand = crypto:strong_rand_bytes(10),
  Uuid = generate(TsMs, Rand),
  binary:encode_hex(Uuid).

generate(UnixTsMs, <<RandA:12, RandB:62, _:6>>) ->
    Ver = 2#0111, Var = 2#10,
    <<UnixTsMs:48, Ver:4, RandA:12, Var:2, RandB:62>>.


