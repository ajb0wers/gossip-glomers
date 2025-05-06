# Gossip Glomers

> Everything looks good! ヽ(‘ー\`)ノ


## Evaluation

```Bash
# Echo
./maelstrom test -w echo --bin /app/echo --node-count 5 --time-limit 10

# Unique ID Generation
./maelstrom test -w unique-ids --bin ../uniqueids \
  --time-limit 30 --rate 1000 --node-count 3 \
  --availability total --nemesis partition

# Single-Node Broadcast
./maelstrom test -w broadcast --bin ../broadcast \
  --node-count 1 --time-limit 20 --rate 10

# Multi-Node Broadcast
./maelstrom test -w broadcast --bin ../broadcast \
  --node-count 5 --time-limit 20 --rate 10

# maelstrom ruby demo using asdf-vm
./maelstrom test -w broadcast --bin $(asdf which ruby) \
  --node-count 5 --time-limit 20 --rate 10 ${PWD}/demo/ruby/broadcast.rb
```


## Podman

```Bash
podman build -t ajb0wers/gossip-glomers .
podman run -it --rm -p 8080:8080 -w /app/maelstrom ajb0wers/gossip-glomers
./maelstrom test -w echo --bin /app/bin/echo --node-count 5 --time-limit 10
```

## See also

- [Gossip Glomers][1] - Fly\.io
- [Maelstrom][2] - github\.com

[1]: https://www.fly.io/dist-sys/
[2]: https://github.com/jepsen-io/maelstrom/
