#!/usr/bin/env bash
set -euo pipefail

kafka() {
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n0","n1"]}}'
  # {"body":{"in_reply_to":1,"type":"init_ok"},"dest":"c1","src":"n1"}

  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":2,"type":"send","key":"k1","msg":9}}'
  # {"body":{"create_if_not_exists":true,"from":-1,"key":"k1","msg_id":1,"to":0,"type":"cas"},"dest":"lin-kv","src":"n1"}
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":1,"type":"cas_ok"}}'
  # {"body":{"key":["k1",0],"msg_id":2,"type":"write","value":9},"dest":"seq-kv","src":"n1"}
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":2,"type":"write_ok"}}'
  # {"body":{"in_reply_to":2,"offset":0,"type":"send_ok"},"dest":"c1","src":"n1"}
  
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":3,"type":"send","key":"k1","msg":5}}'
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":3,"type":"read_ok","value":1000}}'
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":4,"type":"cas_ok"}}'
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":5,"type":"write_ok"}}'
   
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":7,"type":"send","key":"k2","msg":7}}'
  # {"body":{"key":"k2","msg":7,"msg_id":6,"type":"send"},"dest":"n0","src":"n1"}
  printf "%s\n" '{"src":"n0","dest":"n1","body":{"in_reply_to":6,"type":"send_ok","offset":2000}}'
  # {"body":{"in_reply_to":7,"offset":2000,"type":"send_ok"},"dest":"c1","src":"n1"}
   
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":10,"type":"send","key":"k2","msg":2}}'
  printf "%s\n" '{"src":"n0","dest":"n1","body":{"in_reply_to":7,"type":"send_ok","offset":2001}}'

  # k1: [[0,9],[1001,5]]
  # k2: [[2000,7],[2001,2]]
  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":13,"type":"poll","offsets":{"k1":1001,"k2":2000}}}'
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":8,"type":"read_ok","value":5}}'
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":9,"type":"error","code":20}}'
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":10,"type":"read_ok","value":7}}'
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":11,"type":"read_ok","value":2}}'
  printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":12,"type":"error","code":20}}'
 

  printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":18,"type":"commit_offsets","offsets":{"k1":1,"k2":2000}}}'
  # {"body":{"msg_id":13,"offsets":{"k2":2000},"type":"commit_offsets"},"dest":"n0","src":"n1"}
  printf "%s\n" '{"src":"n0","dest":"n1","body":{"in_reply_to":13,"type":"commit_offsets_ok"}}'
  # {"body":{"key":["commit_offset","k1"],"msg_id":14,"type":"read"},"dest":"lin-kv","src":"n1"}
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":14,"type":"read_ok","value":0}}'
  # {"body":{"create_if_not_exists":true,"from":0,"key":["commit_offset","k1"],"msg_id":15,"to":1,"type":"cas"},"dest":"lin-kv","src":"n1"}
  printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":15,"type":"cas_ok"}}'
  # {"body":{"in_reply_to":18,"type":"commit_offsets_ok"},"dest":"c1","src":"n1"}
  
   
  # printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":22,"type":"list_committed_offsets","keys":["k1","k2"]}}'
  # # {"body":{"key":["commit_offset","k1"],"msg_id":22,"type":"read"},"dest":"lin-kv","src":"n1"}
  # printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":22,"type":"read_ok","value":1}}'
  # # {"body":{"key":["commit_offset","k2"],"msg_id":23,"type":"read"},"dest":"lin-kv","src":"n1"}
  # printf "%s\n" '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":23,"type":"read_ok","value":2000}}'
  # # {"body":{"in_reply_to":22,"offsets":{"k1":1,"k2":2000},"type":"list_committed_offsets_ok"},"dest":"c1","src":"n1"}
 
  # printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":25,"type":"poll","offsets":{"666":101}}}'
  # # {"body":{"key":["666",101],"msg_id":24,"type":"read"},"dest":"seq-kv","src":"n1"}
  # printf "%s\n" '{"src":"seq-kv","dest":"n1","body":{"in_reply_to":24,"type":"error","code":20}}'
  # # {"body":{"in_reply_to":25,"msgs":{},"type":"poll_ok"},"dest":"c1","src":"n1"}
  # printf "%s\n" '{"src":"c1","dest":"n1","body":{"msg_id":26,"type":"poll","offsets":{}}}'
  # # {"body":{"in_reply_to":26,"msgs":{},"type":"poll_ok"},"dest":"c1","src":"n1"}

  cat -
} 

"${@:-kafka}" | ./kafka.erl



