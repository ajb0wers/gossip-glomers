#!/usr/bin/env -S escript -c
-module(txn).
-moduledoc """
Challenge #6a: Challenge #6a: Single-Node, Totally-Available Transactions
https://www.fly.io/dist-sys/6a/
""".

-define(CRASH, 13).
-define(ABORT, 14).
-define(KEY_DOES_NOT_EXIST, 20).
-define(PRECONDITION_FAILED, 22).
-define(RPC_ERR(Code, Body), map_get(~"code", Body) == Code).
-define(KEY_DOES_NOT_EXIST(Body), ?RPC_ERR(?KEY_DOES_NOT_EXIST, Body)).
-define(RPC_PRECONDITION_FAILED(Body), ?RPC_ERR(?PRECONDITION_FAILED, Body)).

main([]) ->
  io:setopts(standard_io, [{binary, true}]),
  RpcOutPid = spawn_link(fun rpc_out/0),
  register(rpcout, RpcOutPid),
  ServerPid = spawn_link(fun handle_msg/0),
  register(server, ServerPid),
  loop(standard_io).

%%%%%%%%%%%%%%%%%%%%%%%
%%% Server Handlers %%%
%%%%%%%%%%%%%%%%%%%%%%%
-type nodeid() :: binary().
-type msgid()  :: non_neg_integer().
-type key()    :: binary().
-type value() :: non_neg_integer().
-record #state{
  id        = null :: 'null' | nodeid(),
  nodes     = []   :: [nodeid()],
  callbacks = #{}  :: #{msgid() := any()},
  data      = #{}  :: #{key() := value()}
}.

handle_msg() -> server(fun handle_msg/2, #state{}).

handle_msg(Line, State)  when is_binary(Line) ->
  {noreply, State, parse_line(Line)};

handle_msg({~"init", Src, _Dest, Body}, State) ->
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
handle_msg({~"txn", Src, _Dest, Body}, State) ->
  #{<<"msg_id">> := MsgId, <<"txn">> := Ops} = Body,
  Data0 = State#state.data,

  {Txn, NewData} = lists:foldl(fun
    ([~"r", K, null], {List, Data}) ->
      V = maps:get(K, Data, null),
      {[[~"r", K, V] | List], Data};
    ([~"w", K, V], {List, Data}) ->
      {[[~"w", K, V] | List], Data#{K => V}}
  end, {[], Data0}, Ops),

  NewState = State#state{data = NewData},

  reply(Src, #{
    <<"type">>        => ~"txn_ok",
    <<"in_reply_to">> => MsgId,
    <<"txn">>         => lists:reverse(Txn)
  }, NewState);
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

%%%%%%%%%%%%%%%%%%%%%%%
%%% Server Protocol %%%
%%%%%%%%%%%%%%%%%%%%%%%

loop(standard_io) ->
  case io:get_line([]) of
    eof -> ok;
    {error, Reason} -> exit(Reason);
    Line ->
      server ! Line,
      loop(standard_io)
  end.

rpc_out() ->
  receive
    Msg ->
      Reply = json:encode(Msg),
      io:fwrite("~s~n", [Reply]),
      rpc_out()
  end.

server(Fn, State) ->
  receive
    Msg -> server_call(Fn, Msg, State)
  end.

server_call(Fn, Request, State) ->
  Reply = Fn(Request, State),
  server_reply(Fn, Reply).

server_reply(Fn, {ok, State}) ->
  server(Fn, State);
server_reply(Fn, {reply, Reply, State}) ->
  rpcout ! Reply,
  server(Fn, State);
server_reply(Fn, {noreply, State, Info}) ->
  server_call(Fn, Info, State);
server_reply(Fn, {reply, Reply0, State, Info}) ->
  rpcout ! Reply0,
  server_call(Fn, Info, State);
server_reply(_Fn, stop) ->
  ok.

reply(Dest, Body, #state{} = State) ->
  Reply = #{
    <<"dest">> => Dest,
    <<"src">>  => State#state.id,
    <<"body">> => Body},
  {reply, Reply, State}.

%% reply(Dest, Body, #state{} = State, {_Fun, _Info} = EventData) ->
%%   Request = #{
%%     <<"dest">> => Dest,
%%     <<"src">>  => State#state.id,
%%     <<"body">> => Body},
%%   {noreply, State, {rpc, Request, EventData}}.

parse_line(Line) ->
  Msg = json:decode(Line),
  #{<<"src">> := Src,
    <<"dest">> := Dest,
    <<"body">> := Body} = Msg,
  #{<<"type">> := Type} = Body,
  {Type, Src, Dest, Body}.

