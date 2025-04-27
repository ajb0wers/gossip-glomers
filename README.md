# Gossip Glomers

## Echo

```Bash
./maelstrom test -w echo --bin ~/Desktop/glomers/echo.erl \
  --node-count 5 --time-limit 10
```

## Unique ID Generation

```Bash
./maelstrom test -w unique-ids --bin ~/Desktop/glomers/uniqueids.erl \
  --time-limit 30 --rate 1000 --node-count 3 \
  --availability total --nemesis partition
```

## Single-Node Broadcast

```Bash
./maelstrom test -w broadcast --bin ~/Desktop/glomers/broadcast.erl \
  --node-count 1 --time-limit 20 --rate 10
```

## See also

- [Gossip Glomers][1] - Fly\.io
- [Maelstrom][2] - github\.com

> Everything looks good! ヽ(‘ー\`)ノ

[1]: https://www.fly.io/dist-sys/
[2]: https://github.com/jepsen-io/maelstrom/
