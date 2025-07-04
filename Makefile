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
