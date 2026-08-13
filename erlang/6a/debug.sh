#!/usr/bin/env bash
set -euo pipefail


txn() {
  send '{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n0","n1"]}}'
  # {"body":{"in_reply_to":1,"type":"init_ok"},"dest":"c1","src":"n1"}

  cat -
} 

send() { printf "%s\n" "${1}"; }

"${@:-txn}" | stdbuf -oL ./txn.erl




