#!/usr/bin/env bash
set -euo pipefail

printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n1"]}}'
printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":4,"type":"topology","topology":{"n1":["n2","n3"],"n2":["n1"],"n3":["n1"]}}}'
printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":5,"type":"broadcast","message":1}}'
printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":6,"type":"broadcast","message":2}}'
printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":7,"type":"broadcast","message":3}}'
printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":8,"type":"read"}}'
sleep 1s;
printf "%s\n" '{"src":"n2","dest":"n1","body":{"in_reply_to":6,"type":"broadcast_ok"}}'
sleep 1s;
printf "%s\n" '{"src":"n3","dest":"n1","body":{"in_reply_to":7,"type":"broadcast_ok"}}'
cat -
