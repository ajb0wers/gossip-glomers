printf "%s\n" \
'{"src":"c1","dest":"n1","body":{"msg_id":1,"type":"init","node_id":"n1","node_ids":["n1"]}}' \
'{"src":"c1","dest":"n1","body":{"msg_id":3,"type":"generate"}}' \
| ./uniqueids.erl
