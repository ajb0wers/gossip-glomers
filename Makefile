.PHONY: all check

all: check

check:
	escript -s erlang/1/maelstrom-echo.erl
	escript -s erlang/2/uniqueids.erl
	escript -s erlang/3/broadcast.erl
	escript -s erlang/3e/broadcast.erl
	escript -s erlang/4/g_set.erl
	escript -s erlang/4/counter.erl
	escript -s erlang/4/pn_counter.erl
	escript -s erlang/5a/kafka.erl

serve:
	@cd maelstrom; ./maelstrom serve


echo:
	@echo 'Challenge #1: Echo'
	@cd maelstrom; \
	./maelstrom test -w echo --bin ../erlang/1/maelstrom-echo.erl \
		--node-count 5 --time-limit 10

unique-ids:
	@echo 'Challenge #2: Unique ID Generation'
	@cd maelstrom; \
	./maelstrom test -w unique-ids --bin ../erlang/2/uniqueids.erl \
		--time-limit 30 --rate 1000 --node-count 3 \
		--availability total --nemesis partition
 
broadcast:
	@echo 'Challenge #3d: Efficient Broadcast, Part I'
	@cd maelstrom; \
	./maelstrom test -w broadcast --bin ../erlang/3/broadcast.erl \
		--node-count 25 --time-limit 20 --rate 100 --latency 100 \
		--topology tree4

broadcast-3e:
	@echo 'Challenge #3e: Efficient Broadcast, Part II'
	@cd maelstrom; \
	./maelstrom test -w broadcast --bin ../erlang/3e/broadcast.erl \
		--node-count 25 --time-limit 20 --rate 100 --latency 100

g-counter:
	@echo 'Challenge #4: Grow-Only Counter'
	@cd maelstrom; \
	./maelstrom test -w g-counter --bin ../erlang/4/counter.erl \
		--node-count 3 --rate 100 --time-limit 20 --nemesis partition

g-set:
	@cd maelstrom; \
	./maelstrom test -w g-set --bin ../erlang/4/g_set.erl --time-limit 20 --rate 10

pn-counter:
	@cd maelstrom; \
	./maelstrom test -w pn-counter --bin ../erlang/4/pn_counter.erl \
		--time-limit 20 --rate 10 

kafka-5a:
	@echo 'Challenge #5a: Single-Node Kafka-Style Log'
	@cd maelstrom; \
	./maelstrom test -w kafka --bin ../erlang/5a/kafka.erl \
		--node-count 1 --concurrency 2n --time-limit 20 --rate 1000
