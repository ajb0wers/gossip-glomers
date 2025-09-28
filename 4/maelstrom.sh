#!/usr/bin/env bash
set -euo pipefail

g-set() {
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n1","n2","n3"]}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":5,"type":"add","element":1}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":6,"type":"add","element":2}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":7,"type":"add","element":3}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":8,"type":"read"}}'
  sleep 1s;
  printf "%s\n" '{"src":"n2","dest":"n1","body":{"msg_id":9,"type":"broadcast","message":[1,2,3,4,5,6]}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":10,"type":"read"}}'
  cat -
}

g-counter() {
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n1","n2","n3"]}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":5,"type":"add","delta":2}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":6,"type":"add","delta":4}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":7,"type":"add","delta":6}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":8,"type":"read"}}'
  sleep 1s;
  printf "%s\n" '{"src":"n2","dest":"n1","body":{"msg_id":9,"type":"broadcast","message":{"n1":20, "n2":100}}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":10,"type":"read"}}'
  cat -
}

pn-counter() {
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n1","n2","n3"]}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":5,"type":"add","delta":1}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":6,"type":"add","delta":2}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":7,"type":"add","delta":-3}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":8,"type":"read"}}'
  sleep 1s;
  printf "%s\n" '{"src":"n2","dest":"n1","body":{"msg_id":9,"type":"broadcast","message":{"inc": {"n1":10, "n2":1}, "dec":{"n1":5}}}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":10,"type":"read"}}'
  cat -
}

"$@"
