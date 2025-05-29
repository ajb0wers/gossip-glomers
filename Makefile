.PHONY: all maelstrom escript

all: check-escript

check-escript:
	escript -s echo.erl
	escript -s uniqueids.erl
	escript -s broadcast.erl

maelstrom: check-escript
	podman build -t ajb0wers/gossip-glomers .
	podman run -it --rm -p 8080:8080 -w /app/maelstrom ajb0wers/gossip-glomers
