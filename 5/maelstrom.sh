#!/usr/bin/env bash
set -euo pipefail

kafka() {
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n1","n2","n3"]}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"type":"send","key":"k1","msg":9}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"type":"send","key":"k2","msg":7}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"type":"send","key":"k2","msg":2}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"type":"send","key":"k1","msg":5}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"type":"poll","offsets":{"k1":2,"k2":1}}}'
  cat -
} 

"$@" | ./"${1}".erl
