#!/usr/bin/env escript

-define(PROMPT, "").

main([]) -> 
  io:setopts(standard_io, [{binary, true}]),
  loop(null).

loop(State) ->
  case io:get_line(?PROMPT) of
    Line when is_binary(Line) -> 
      Msg = json:decode(Line),
      {Reply, NewState} = handle(Msg, State),
      MsgOut = json:encode(Reply),
      io:fwrite("~s~n", [MsgOut]),
      loop(NewState);
    _Eof -> ok
  end.

handle(Msg, NodeId) ->
  #{<<"src">>  := Src,
    <<"dest">> := Dest,
    <<"body">> := Body} = Msg,
  #{<<"type">> := Tag} = Body,
  handle(Tag, {Src, Dest, Body}, NodeId).

handle(<<"init">>, {Src, Dest, Body}, _NodeId0) ->
  #{<<"type">>     := <<"init">>,
    <<"msg_id">>   := MsgId,
    <<"node_id">>  := NodeId,
    <<"node_ids">> := _NodeIds} = Body,

  Reply = #{
    <<"src">>  => Dest,
    <<"dest">> => Src, 
    <<"body">> => #{
      <<"type">> => <<"init_ok">>,
      <<"in_reply_to">> => MsgId
  }},
  {Reply, NodeId};

handle(<<"echo">>, {Src, Dest, Body}, NodeId) ->
  #{<<"type">> := <<"echo">>,
    <<"msg_id">> := MsgId,
    <<"echo">> := Data} = Body,

  Reply = #{
    <<"src">>  => Dest,
    <<"dest">> => Src, 
    <<"body">> => #{
      <<"type">> => <<"echo_ok">>,
      <<"msg_id">> => erlang:unique_integer([monotonic, positive]), 
      <<"in_reply_to">> => MsgId,
      <<"echo">> => Data
  }},

  {Reply, NodeId}.
