#!/usr/bin/env bash
set -euo pipefail


txn() {
  send '{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n0","n1"]}}'
  # {"body":{"in_reply_to":1,"type":"init_ok"},"dest":"c1","src":"n1"}

  send '{"src":"c1","dest":"n1","body":{"type":"txn","msg_id":2,"txn":[["w",1,3]]}}'
  send '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":1,"type":"error","code":20}}'
  send '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":2,"type":"write_ok"}}'
  send '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":3,"type":"cas_ok"}}'
  # {"body":{"in_reply_to":2,"txn":[["w",1,3]],"type":"txn_ok"},"dest":"c1","src":"n1"} 

  send '{"src":"c1","dest":"n1","body":{"type":"txn","msg_id":3,"txn":[["r",1,null],["w",1,6],["w",2,9]]}}'
  send '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":4,"type":"read_ok","value":"01A073DDB4307810A6696E4F1A615626"}}'
  send '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":5,"type":"read_ok","value":{"1":3}}}'
  send '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":6,"type":"write_ok"}}'
  send '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":7,"type":"error","code":22}}'
  sleep 0.5s
  send '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":8,"type":"read_ok","value":"01A07742E98F7E048DE0D01EF8E0567B"}}'
  send '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":9,"type":"read_ok","value":{"1":3}}}'
  send '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":10,"type":"write_ok"}}'
  send '{"src":"lin-kv","dest":"n1","body":{"in_reply_to":11,"type":"cas_ok"}}'
  # {"body":{"in_reply_to":3,"txn":[["r",1,null],["w",1,6],["w",2,9]],"type":"txn_ok"},"dest":"c1","src":"n1"}

  cat -
} 

send() { printf "%s\n" "${1}"; }

"${@:-txn}" | stdbuf -oL ./txn.erl




