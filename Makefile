.PHONY: all check lint serve podman

all: check

check:
	escript -s erlang/1/echo.erl
	escript -s erlang/2/uniqueids.erl
	escript -s erlang/3/broadcast.erl
	escript -s erlang/3e/broadcast.erl
	escript -s erlang/4/g_set.erl
	escript -s erlang/4/counter.erl
	escript -s erlang/4/pn_counter.erl
	escript -s erlang/5a/kafka.erl
	escript -s erlang/5b/kafka.erl
	escript -s erlang/5c/kafka.erl
	escript -s erlang/6a/txn.erl

lint: check
	elvis rock

serve:
	@cd maelstrom; ./maelstrom serve

podman:
	podman build -t ajb0wers/gossip-glomers .
	podman run -it --rm -p 8080:8080 -w /app/ ajb0wers/gossip-glomers

# Challenge #1: Echo
echo:
	@cd maelstrom; \
	./maelstrom test -w echo --bin ../erlang/1/echo.erl \
		--node-count 5 --time-limit 10

# Challenge #2: Unique ID Generation
unique-ids:
	@cd maelstrom; \
	./maelstrom test -w unique-ids --bin ../erlang/2/uniqueids.erl \
		--time-limit 30 --rate 1000 --node-count 3 \
		--availability total --nemesis partition
 
# Challenge #3d: Efficient Broadcast, Part I
broadcast:
	@cd maelstrom; \
	./maelstrom test -w broadcast --bin ../erlang/3/broadcast.erl \
		--node-count 25 --time-limit 20 --rate 100 --latency 100 \
		--topology tree4

# Challenge #3e: Efficient Broadcast, Part II
broadcast-3e:
	@cd maelstrom; \
	./maelstrom test -w broadcast --bin ../erlang/3e/broadcast.erl \
		--node-count 25 --time-limit 20 --rate 100 --latency 100

# Challenge #4: Grow-Only Counter
g-counter:
	@cd maelstrom; \
	./maelstrom test -w g-counter --bin ../erlang/4/counter.erl \
		--node-count 3 --rate 100 --time-limit 20 --nemesis partition

# Challenge #5a: Single-Node Kafka-Style Log
kafka-5a:
	@cd maelstrom; \
	./maelstrom test -w kafka --bin ../erlang/5a/kafka.erl \
		--node-count 1 --concurrency 2n --time-limit 20 --rate 1000

# Challenge #5b: Multi-Node Kafka-Style Log
kafka-5b:
	@cd maelstrom; \
	./maelstrom test -w kafka --bin ../erlang/5b/kafka.erl \
		--node-count 2 --concurrency 2n --time-limit 20 --rate 1000

# Challenge #5c: Efficient Kafka-Style Log
kafka-5c:
	@cd maelstrom; \
	./maelstrom test -w kafka --bin ../erlang/5c/kafka.erl \
		--node-count 2 --concurrency 2n --time-limit 20 --rate 1000

# Challenge #6a: Challenge #6a: Single-Node, Totally-Available Transactions
txn-6a:
	@cd maelstrom; \
	./maelstrom test -w txn-rw-register --bin ../erlang/6a/txn.erl \
		--node-count 1 --time-limit 20 --rate 1000 --concurrency 2n \
		--consistency-models read-uncommitted --availability total

# Maelstrom CRDTs G-set
g-set:
	@cd maelstrom; \
	./maelstrom test -w g-set --bin ../erlang/4/g_set.erl \
		--time-limit 20 --rate 10

# Maelstrom CRDTs PN-Counters
pn-counter:
	@cd maelstrom; \
	./maelstrom test -w pn-counter --bin ../erlang/4/pn_counter.erl \
		--time-limit 20 --rate 10 

