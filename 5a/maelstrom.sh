#!/usr/bin/env bash
set -euo pipefail

kafka() {
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n1","n2","n3"]}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":2,"type":"send","key":"k1","msg":9}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":3,"type":"send","key":"k1","msg":5}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":4,"type":"send","key":"k2","msg":7}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":5,"type":"send","key":"k2","msg":2}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":6,"type":"poll","offsets":{"k1":2,"k2":1}}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":7,"type":"commit_offsets","offsets":{"k1":1,"k2":2}}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":8,"type":"list_committed_offsets","keys":["k1","k2"]}}'
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":9,"type":"poll","offsets":{"k1":2,"k2":2}}}'
  cat -
} 

"$@" | ./"${1}".erl
