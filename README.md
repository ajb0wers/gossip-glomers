# Gossip Glomers

> Everything looks good! ヽ(‘ー\`)ノ


## Evaluation

```Bash
# Echo
./maelstrom test -w echo --bin ../echo.erl --node-count 5 --time-limit 10

# Unique ID Generation
./maelstrom test -w unique-ids --bin ../uniqueids.erl \
  --time-limit 30 --rate 1000 --node-count 3 \
  --availability total --nemesis partition

# Single-Node Broadcast
./maelstrom test -w broadcast --bin ../broadcast.erl \
  --node-count 1 --time-limit 20 --rate 10
```

## Podman

```bash
podman build -t ajb0wers/gossip-glomers .
podman run -it --rm -w /app/maelstrom ajb0wers/gossip-glomers
./maelstrom test -w echo --bin /app/echo.erl --node-count 5 --time-limit 10
```

## See also

- [Gossip Glomers][1] - Fly\.io
- [Maelstrom][2] - github\.com

[1]: https://www.fly.io/dist-sys/
[2]: https://github.com/jepsen-io/maelstrom/
