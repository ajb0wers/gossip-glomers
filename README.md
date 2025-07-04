# Gossip Glomers

> Everything looks good! ヽ(‘ー\`)ノ


## Evaluation

```Bash
# Echo
./maelstrom test -w echo --bin ../1/echo.erl --node-count 5 --time-limit 10

# Unique ID Generation
./maelstrom test -w unique-ids --bin ../2/uniqueids.erl \
  --time-limit 30 --rate 1000 --node-count 3 \
  --availability total --nemesis partition

# Multi-Node Broadcast
./maelstrom test -w broadcast --bin ../3e/broadcast.erl \
  --node-count 25 --time-limit 20 --rate 100 --latency 100

# Running `maelstrom` in an Erlang container:
podman build -t ajb0wers/gossip-glomers .
podman run -it --rm -p 8080:8080 -w /app/maelstrom ajb0wers/gossip-glomers
./maelstrom test -w echo --bin /app/bin/echo.erl --node-count 5 --time-limit 10
```

## See also

- [Gossip Glomers][1] - Fly\.io
- [Maelstrom][2] - github\.com

[1]: https://www.fly.io/dist-sys/
[2]: https://github.com/jepsen-io/maelstrom/
