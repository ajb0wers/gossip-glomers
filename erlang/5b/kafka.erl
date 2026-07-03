#!/usr/bin/env -S escript -c
-module(kafka).

-export([init/1]).

-define(KEY_DOES_NOT_EXIST, 20).
-define(PRECONDITION_FAILED, 22).

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
  spawn(?MODULE, init, [server]),
  spawn(?MODULE, init, [rpcout]),
  loop(standard_io).

init(rpcout) -> 
  register(rpcout, self()),
  rpcout();
init(server) ->
  register(server, self()),
  server(fun handle_msg/2, #state{}).

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
      handle_reply(Fn, Reply);
    Msg ->
      Reply = Fn(Msg, State),
      handle_reply(Fn, Reply)
  end.

handle_reply(Fn, {ok, State}) ->
  server(Fn, State);
handle_reply(Fn, {reply, Reply, State}) ->
  rpcout ! Reply,
  server(Fn, State);
handle_reply(Fn, {noreply, State, Info}) ->
  Reply = handle_continue(Info, State),
  handle_reply(Fn, Reply);
handle_reply(Fn, {reply, Reply0, State, Info}) ->
  rpcout ! Reply0,
  Reply = handle_continue(Info, State),
  handle_reply(Fn, Reply);
handle_reply(_Fn, stop) ->
  ok.

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


handle_msg({~"send", Src, Dest, Body} = _Msg, State) ->
  handle_append({append, {Src,Dest,Body}}, State);

%% handle_msg({~"poll", Src, Dest, Body}, #state{data=Logs} = State) ->
%%   #{<<"offsets">> := Offsets, <<"msg_id">> := MsgId} = Body,
%% 
%%   Msgs = read(Offsets, Logs),
%% 
%%   reply(Src, Dest, #{
%%     <<"type">> => <<"poll_ok">>,
%%     <<"msgs">> => Msgs,
%%     <<"in_reply_to">> => MsgId
%%   }, State);
%% 
%% handle_msg({~"commit_offsets", Src, Dest, Body}, #state{data=Data} = State) ->
%%   #{<<"offsets">> := Offsets, <<"msg_id">> := MsgId} = Body,
%% 
%%   NewData = commit(Offsets, Data),
%%   NewState = State#state{data=NewData},
%% 
%%   reply(Src, Dest, #{
%%     <<"type">> => <<"commit_offsets_ok">>,
%%     <<"in_reply_to">> => MsgId
%%   }, NewState);
%% 
%% handle_msg({~"list_committed_offsets", Src, Dest, Body}, State) ->
%%   #{<<"keys">> := Ks, <<"msg_id">> := MsgId} = Body,
%% 
%%   Offsets = list(Ks, State#state.data),
%% 
%%   reply(Src, Dest, #{
%%     <<"type">> => <<"list_committed_offsets_ok">>,
%%     <<"offsets">> => Offsets,
%%     <<"in_reply_to">> => MsgId
%%   }, State);
%% 

handle_msg({_,_,_, #{~"in_reply_to" := ReplyId}} = Msg, State) ->
  #{ReplyId := {F, CallbackInfo}} = State#state.append_id, 
  Callbacks0 = State#state.append_id,
  Callbacks = maps:remove(ReplyId, Callbacks0),
  NewState = State#state{append_id=Callbacks},
  erlang:apply(?MODULE, F, [{Msg, CallbackInfo}, NewState]);
handle_msg({_Tag, _Src, _Dest}, State) -> {ok, State}.

handle_continue(_Info, State) -> {ok, State}.

handle_append({append, Msg}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  {_, _, #{<<"key">> := Key}} = Msg,
  Callbacks0 = State#state.append_id,
  Callbacks = Callbacks0#{MsgId => {handle_append, Msg}},
  NewState = State#state{append_id=Callbacks},
  reply(~"lin-kv", #{
    <<"type">>   => <<"read">>,
    <<"key">>    => Key,
    <<"msg_id">> => MsgId
  }, NewState);
handle_append({cas, Msg}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  {_, _, #{<<"key">> := Key}} = Msg,
  Value = 0,
  N = 1, To = Value + N,

  Callbacks0 = State#state.append_id,
  Callbacks = Callbacks0#{MsgId => {handle_append, {{Key,Value,To,N}, Msg}}},
  NewState = State#state{append_id=Callbacks},

  reply(~"lin-kv", #{
    <<"type">>   => <<"cas">>,
    <<"key">>    => Key,
    <<"from">>   => Value,
    <<"to">>     => To,
    <<"msg_id">> => MsgId,
    <<"create_if_not_exists">> => true
  }, NewState);
handle_append({{~"read_ok", ~"lin-kv" = Src, Dest, Body}, Info}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  {_,_, #{<<"key">> := Key}} = Info,
  #{~"value" := Value} = Body,
  N = 1, To = Value + N,

  Callbacks0 = State#state.append_id,
  Callbacks = Callbacks0#{MsgId => {handle_append, {{Key,Value,To,N}, Info}}},
  NewState = State#state{append_id=Callbacks},

  reply(Src, Dest, #{
    <<"type">>   => <<"cas">>,
    <<"key">>    => Key,
    <<"from">>   => Value,
    <<"to">>     => To,
    <<"msg_id">> => MsgId,
    <<"create_if_not_exists">> => true
  }, NewState);
handle_append({{~"cas_ok", ~"lin-kv", Dest, _Body}, Info}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  {{Key, _, To, _N}, Msg} = Info,
  {_,_, #{<<"msg">> := Value}} = Msg,

  Callbacks0 = State#state.append_id,
  Callbacks = Callbacks0#{MsgId => {handle_append, {{Key, Value, To}, Msg}}},
  NewState = State#state{append_id=Callbacks},

  reply(~"seq-kv", Dest, #{
    <<"type">> => <<"write">>,
    <<"key">> => [Key,To],
    <<"value">> => Value,
    <<"msg_id">> => MsgId
  }, NewState);
handle_append({{~"error", ~"lin-kv", _Dest, Body}, Info}, State)
  when map_get(~"code", Body) == ?KEY_DOES_NOT_EXIST ->
  handle_append({cas, Info}, State); %% read_ok
handle_append({{~"error", ~"lin-kv", _Dest, Body}, Info}, State)
  when map_get(~"code", Body) == ?PRECONDITION_FAILED ->
  handle_append({append, Info}, State);
handle_append({{~"write_ok", ~"seq-kv", _, _}, Info}, State) ->
  {{_, _, Offset}, Msg} = Info,
  {Src, Dest, #{<<"msg_id">> := MsgId}} = Msg,
  reply(Src, Dest, #{
    <<"type">> => <<"send_ok">>,
    <<"offset">> => Offset,
    <<"in_reply_to">> => MsgId
  }, State).

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

%% append(start, {Msg, MsgId}) ->
%%   {_, NodeId, #{<<"key">> := Key}} = Msg,
%%   Reply = #{
%%     <<"src">> => NodeId,
%%     <<"dest">> => <<"lin-kv">>,
%%     <<"body">> => #{
%%       <<"type">>   => <<"read">>,
%%       <<"key">>    => Key,
%%       <<"msg_id">> => MsgId
%%   }},
%%   {MsgId, Reply, Msg}.
%% 
%% read(Offsets, Logs) ->
%%   maps:fold(fun (K, From, AccIn) ->
%%     case Logs of 
%%       #{K := {_Length, _Commit, Log}} ->
%%         %% Pred = fun({I,_}) -> I >= From andalso I > Commit end,
%%         Pred = fun({I,_}) -> I >= From end,
%%         List = lists:takewhile(Pred, Log),
%%         Queue = lists:reverse([[I,H] || {I,H} <- List]),
%%         AccIn#{K => Queue};
%%       _NoMatch -> AccIn
%%     end
%%   end, #{}, Offsets).
%% 
%% commit(Offsets, Logs) ->
%%   maps:fold(fun (K, End, AccIn) -> 
%%     case Logs of
%%       #{K := {Offset, Commit, Log}} when End > Commit ->
%%         AccIn#{K := {Offset, End, Log}};
%%       _ -> AccIn
%%     end
%%   end, Logs, Offsets).
%% 
%% list(Keys, Logs) ->
%%   Map = maps:with(Keys, Logs),
%%   #{K => Commit || K := {_,Commit,_} <- Map}.
