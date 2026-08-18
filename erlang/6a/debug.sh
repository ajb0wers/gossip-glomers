#!/usr/bin/env bash
set -euo pipefail


txn() {
  send '{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n0","n1"]}}'
  # {"body":{"in_reply_to":1,"type":"init_ok"},"dest":"c1","src":"n1"}

  send '{"src":"c1","dest":"n1","body":{"type":"txn","msg_id":2,"txn":[["w",1,3]]}}'
  # {"body":{"in_reply_to":2,"txn":[["w",1,3]],"type":"txn_ok"},"dest":"c1","src":"n1"}

  send '{"src":"c1","dest":"n1","body":{"type":"txn","msg_id":3,"txn":[["r",1,null],["w",1,6],["w",2,9]]}}'
  # {"body":{"in_reply_to":3,"txn":[["r",1,3],["w",1,6],["w",2,9]],"type":"txn_ok"},"dest":"c1","src":"n1"}

  cat -
} 

send() { printf "%s\n" "${1}"; }

"${@:-txn}" | stdbuf -oL ./txn.erl




