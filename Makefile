.PHONY: all maelstrom check

all: check

check:
	escript -s 1/echo.erl
	escript -s 2/uniqueids.erl
	escript -s 3/broadcast.erl
	escript -s 3e/broadcast.erl

maelstrom: check
	podman build -t ajb0wers/gossip-glomers .
	podman run -it --rm -p 8080:8080 -w /app/maelstrom ajb0wers/gossip-glomers

evaluation: check echo unique-ids broadcast broadcast-3e

echo:
	@echo 'Challenge #1: Echo'
	@cd maelstrom; \
	./maelstrom test -w echo --bin ../1/echo.erl --node-count 5 --time-limit 10

unique-ids:
	@echo 'Challenge #2: Unique ID Generation'
	@cd maelstrom; \
	./maelstrom test -w unique-ids --bin ../2/uniqueids.erl \
		--time-limit 30 --rate 1000 --node-count 3 \
		--availability total --nemesis partition
 
broadcast:
	@echo 'Challenge #3d: Efficient Broadcast, Part I'
	@cd maelstrom; \
	./maelstrom test -w broadcast --bin ../3/broadcast.erl \
    --node-count 25 --time-limit 20 --rate 100 --latency 100 \
		--topology tree4

broadcast-3e:
	@echo 'Challenge #3e: Efficient Broadcast, Part II'
	@cd maelstrom; \
	./maelstrom test -w broadcast --bin ../3e/broadcast.erl \
	  --node-count 25 --time-limit 20 --rate 100 --latency 100

