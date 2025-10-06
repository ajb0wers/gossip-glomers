#!/usr/bin/env -S escript -c
-module(pn_counter).

-export([init/0, handle_rpc/1]).

-define(PROMPT, "").
-define(FORMAT, "~s~n").

-record(state, {
	node_id  = null :: 'null' | integer(),
	node_ids = []   :: list(),
  log     = #{}   :: map()
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


handle_msg(~"send", {Src, Dest, Body}, State) ->
  #{<<"key">> := _Key, <<"msg">> := _Msg} = Body,
  NewState = State,
  reply(Src, Dest, #{
    <<"type">> => <<"send_ok">>,
    <<"offset">> => 0
  }, NewState);

handle_msg(~"poll", {Src, Dest, Body}, State) ->
  #{<<"key">> := _Key, <<"msg">> := _Msg } = Body,
  NewState = State,
  reply(Src, Dest, #{
    <<"type">> => <<"poll_ok">>,
    <<"offsets">> => [#{}] 
  }, NewState);

handle_msg(~"commit_offsets", {_Src, _Dest, _Body}, State) -> {ok, State};
handle_msg(~"list_commit_offsets", {_Src, _Dest, _Body}, State) -> {ok, State};

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

handle_rpc(State) ->
  receive
    {rpc, Msg} ->
      io:format(?FORMAT, [json:encode(Msg)]),
      handle_rpc(State);
    {rpc, Msg, Id, Time} ->
      io:format(?FORMAT, [json:encode(Msg)]),
      {ok, TRef} = timer:send_after(Time, {rpc, Msg, Id, Time}),
      NewState = State#{Id => TRef},
      handle_rpc(NewState);
    {ok, Id} ->
      #{Id := Timer} = State,
      timer:cancel(Timer),
      NewState = maps:remove(Id, State),
      handle_rpc(NewState);
    {reply, Msg} ->
      io:format(?FORMAT, [json:encode(Msg)]),
      handle_rpc(State);
    _ ->
      handle_rpc(State)
  end.
