#!/usr/bin/env -S escript -c
-module(kafka).

-export([init/1, handle_append/2, handle_poll/2]).

-define(KEY_DOES_NOT_EXIST, 20).
-define(PRECONDITION_FAILED, 22).

-record(state, {
  node_id   = null :: 'null' | binary(),
  node_ids  = []   :: [binary()],
  % append_msg = #{} :: #{Key::binary() := Values::list()},
  callbacks = #{} :: #{MsgId::non_neg_integer() := any()},
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
  Reply = handle_continue(Info, State),
  server_reply(Fn, Reply);
server_reply(Fn, {reply, Reply0, State, Info}) ->
  rpcout ! Reply0,
  Reply = handle_continue(Info, State),
  server_reply(Fn, Reply);
server_reply(_Fn, stop) ->
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


handle_msg({~"send", _Src, _Dest, _Body} = Msg, State) ->
  handle_append(Msg, State);

handle_msg({~"poll", _Src, _Dest, _Body} = Msg, State) ->
  handle_poll(Msg, State);

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
  #{ReplyId := {F, CallbackInfo}} = State#state.callbacks, 
  Callbacks0 = State#state.callbacks,
  Callbacks = maps:remove(ReplyId, Callbacks0),
  NewState = State#state{callbacks=Callbacks},
  erlang:apply(?MODULE, F, [{Msg, CallbackInfo}, NewState]);
handle_msg({_Tag, _Src, _Dest}, State) -> {ok, State}.

handle_continue(_Info, State) -> {ok, State}.

handle_append({~"send", _, _, Body} = Send, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  #{<<"key">> := Key} = Body,
  Callbacks0 = State#state.callbacks,
  Callbacks = Callbacks0#{MsgId => {handle_append, Send}},
  NewState = State#state{callbacks=Callbacks},
  reply(~"lin-kv", #{
    <<"type">>   => <<"read">>,
    <<"key">>    => Key,
    <<"msg_id">> => MsgId
  }, NewState);
handle_append({{~"read_ok", ~"lin-kv", _, Body}, Send}, State) ->
  #{~"value" := Value} = Body,
  handle_append({cas, Value, Send}, State);
handle_append({{~"error", ~"lin-kv", _Dest, Body}, Send}, State)
  when map_get(~"code", Body) == ?KEY_DOES_NOT_EXIST ->
  %% handle link-kv rpc read error (20) when key doesn't exist.
  %% TODO: expect `in_reply_to=msg_id`.
  handle_append({cas, 0, Send}, State);
handle_append({cas, From, Send}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  {_, _, #{<<"key">> := Key}} = Send,
  N = 1, Offset = From + N,

  Callbacks0 = State#state.callbacks,
  Callbacks = Callbacks0#{MsgId => {handle_append, {{Key,From,Offset,N}, Send}}},
  NewState = State#state{callbacks=Callbacks},

  reply(~"lin-kv", #{
    <<"type">>   => <<"cas">>,
    <<"key">>    => Key,
    <<"from">>   => From,
    <<"to">>     => Offset,
    <<"msg_id">> => MsgId,
    <<"create_if_not_exists">> => true
  }, NewState);
handle_append({{~"cas_ok", ~"lin-kv", Dest, _Body}, Info}, State) ->
  MsgId = erlang:unique_integer([monotonic, positive]), 
  {{Key, _, Offset, _N}, Msg} = Info,
  {_,_, #{<<"msg">> := Value}} = Msg,

  Callbacks0 = State#state.callbacks,
  Callbacks = Callbacks0#{MsgId => {handle_append, {{Key,Value,Offset}, Msg}}},
  NewState = State#state{callbacks=Callbacks},

  reply(~"seq-kv", Dest, #{
    <<"type">> => <<"write">>,
    <<"key">> => [Key,Offset],
    <<"value">> => Value,
    <<"msg_id">> => MsgId
  }, NewState);
handle_append({{~"error", ~"lin-kv", _Dest, Body}, Send}, State)
  when map_get(~"code", Body) == ?PRECONDITION_FAILED ->
  %% handle lin-kv rpc cas error (22) when from value doesn't match.
  %% TODO: expect `in_reply_to=msg_id`.
  handle_append(Send, State);
handle_append({{~"write_ok", ~"seq-kv", _, _}, Info}, State) ->
  {{_, _, Offset}, Msg} = Info,
  {Src, Dest, #{<<"msg_id">> := MsgId}} = Msg,
  reply(Src, Dest, #{
    <<"type">> => <<"send_ok">>,
    <<"offset">> => Offset,
    <<"in_reply_to">> => MsgId
  }, State).

handle_poll({~"poll", _Src, _Dest, _Body} = _Msg, State) -> 
  %% #{<<"offsets">> := Offsets, <<"msg_id">> := MsgId} = Body,
  %% I = maps:iterator(Offsets),
  %% Info = {I, Offsets, Msg},
  %% Callbacks0 = State#state.callbacks,
  %% Callbacks = Callbacks0#{MsgId => {handle_poll, Info},
  %% NewState = State#state{callbacks=Callbacks},
  {ok, State}. 



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
