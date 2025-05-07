.PHONY: all maelstrom

all:
	escript -s echo
	escript -s uniqueids
	escript -s broadcast

maelstrom:
	podman build -t ajb0wers/gossip-glomers .
	podman run -it --rm -p 8080:8080 -w /app/maelstrom ajb0wers/gossip-glomers
