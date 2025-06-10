#!/usr/bin/env bash
printf "%s\n" \
'{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n1"]}}' \
'{"src":"c1","dest":"n1","body":{"msg_id":2,"type":"echo","echo":"hello there"}}' \
'{"src":"c1","dest":"n1","body":{"msg_id":3,"type":"generate"}}' \
'{"src":"c1","dest":"n1","body":{"msg_id":4,"type":"topology","topology":{"n1":["n2","n3"],"n2":["n1"],"n3":["n1"]}}}' \
'{"src":"c1","dest":"n1","body":{"msg_id":5,"type":"broadcast","message":1}}' \
'{"src":"c1","dest":"n1","body":{"msg_id":6,"type":"read"}}'
sleep 0.8s
printf "%s\n" \
'{"src":"n2","dest":"n1","body":{"in_reply_to":4,"type":"broadcast_ok"}}' \
'{"src":"n3","dest":"n1","body":{"in_reply_to":5,"type":"broadcast_ok"}}'
cat -
