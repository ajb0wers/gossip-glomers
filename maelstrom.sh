#!/usr/bin/env bash
cat <<EOF
{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n1"]}}
{"src":"c1","dest":"n1","body":{"msg_id":2,"type":"echo","echo":"hello there"}}
{"src":"c1","dest":"n1","body":{"msg_id":3,"type":"generate"}}
{"src":"c1","dest":"n1","body":{"msg_id":4,"type":"topology","topology":{"n1":["n2","n3"],"n2":["n1"],"n3":["n1"]}}}
{"src":"c1","dest":"n1","body":{"msg_id":5,"type":"broadcast","message":1}}
{"src":"c1","dest":"n1","body":{"msg_id":6,"type":"broadcast","message":2}}
{"src":"c1","dest":"n1","body":{"msg_id":7,"type":"broadcast","message":3}}
{"src":"c1","dest":"n1","body":{"msg_id":8,"type":"read"}}
EOF
sleep 1s; cat <<EOF
{"src":"n2","dest":"n1","body":{"in_reply_to":6,"type":"broadcast_ok"}}
EOF
sleep 1s; cat <<EOF
{"src":"n3","dest":"n1","body":{"in_reply_to":7,"type":"broadcast_ok"}}
EOF
cat -
