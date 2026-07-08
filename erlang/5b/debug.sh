#!/usr/bin/env bash
set -euo pipefail

kafka() {
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n1","n2","n3"]}}'

  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":2,"type":"send","key":"k1","msg":9}}'
  # {"body":{"key":"k1","msg_id":1,"type":"read"},"dest":"lin-kv","src":"n1"}
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":1,"type":"error","code":20}}'
  # {"body":{"create_if_not_exists":true,"from":0,"key":"k1","msg_id":2,"to":1,"type":"cas"},"dest":"lin-kv","src":"n1"}
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":2,"type":"cas_ok"}}'
  # {"body":{"key":["k1",1],"msg_id":3,"type":"write","value":0},"dest":"seq-kv","src":"n1"}
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":3,"type":"write_ok"}}'

  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":3,"type":"send","key":"k1","msg":5}}'
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":4,"type":"read_ok","value":1000}}'
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":5,"type":"cas_ok"}}'
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":6,"type":"write_ok"}}'
  
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":7,"type":"send","key":"k2","msg":7}}'
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":7,"type":"read_ok","value":1999}}'
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":8,"type":"cas_ok"}}'
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":9,"type":"write_ok"}}'
  
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":10,"type":"send","key":"k2","msg":2}}'
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":10,"type":"read_ok","value":2000}}'
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":11,"type":"cas_ok"}}'
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":12,"type":"write_ok"}}'

  # k1: [[1,9],[1001,5]]
  # k2: [[2000,7],[2001,2]]
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":13,"type":"poll","offsets":{"k1":1001,"k2":2000}}}'
  # {"body":{"key":["k1",2],"msg_id":13,"type":"read"},"dest":"seq-kv","src":"n1"}
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":13,"type":"read_ok","value":5}}'
  # {"body":{"key":["k1",3],"msg_id":14,"type":"read"},"dest":"seq-kv","src":"n1"}
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":14,"type":"error","code":20}}'
  # {"body":{"key":["k2",1],"msg_id":15,"type":"read"},"dest":"seq-kv","src":"n1"} 
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":15,"type":"read_ok","value":7}}'
  # {"body":{"key":["k2",2],"msg_id":16,"type":"read"},"dest":"seq-kv","src":"n1"}
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":16,"type":"read_ok","value":2}}'
  # {"body":{"key":["k2",3],"msg_id":17,"type":"read"},"dest":"seq-kv","src":"n1"}
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":17,"type":"error","code":20}}'
  # {"body":{"msgs":{"k1":[[1001,5]],"k2":[[2000,7],[2001,2]]},"type":"poll_ok"},"dest":"c1","src":"n1"}
 
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":18,"type":"commit_offsets","offsets":{"k1":1,"k2":2000}}}'
  # {"body":{"key":["commit_offset","k1"],"msg_id":18,"type":"read"},"dest":"lin-kv","src":"n1"}
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":18,"type":"read_ok","value":0}}'
  # {"body":{"create_if_not_exists":true,"from":0,"key":["commit_offset","k1"],"msg_id":19,"to":1,"type":"cas"},"dest":"lin-kv","src":"n1"}
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":19,"type":"cas_ok"}}'
 
  
  
  # printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":8,"type":"list_committed_offsets","keys":["k1","k2"]}}'
  # printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":9,"type":"poll","offsets":{"k1":2,"k2":2}}}'

  cat -
} 

"${@:-kafka}" | ./kafka.erl
