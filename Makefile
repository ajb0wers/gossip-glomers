.PHONY: all echo uniqueids broadcast

all: echo uniqueids broadcast

echo:
	escript -s echo

uniqueids:
	escript -s uniqueids

broadcast: 
	escript -s broadcast
