# Gossip Glomers

> Everything looks good! ``ヽ(‘ー`)ノ``


## Evaluation

See `Makefile`.

```Bash
# Challenge #1: Echo
make echo

# Challenge #2: Unique ID Generation
make unique-ids

# Challenge #3d: Efficient Broadcast, Part I
make broadcast

# Challenge #3e: Efficient Broadcast, Part II
make broadcast-3e

# Challenge #4: Grow-Only Counter
make g-counter

# Container build & run
podman build -t ajb0wers/gossip-glomers .
podman run -it --rm -p 8080:8080 -w /app/ ajb0wers/gossip-glomers
```

## See also

- [Gossip Glomers][1] - Fly\.io
- [Maelstrom][2] - github\.com

[1]: https://www.fly.io/dist-sys/
[2]: https://github.com/jepsen-io/maelstrom/
