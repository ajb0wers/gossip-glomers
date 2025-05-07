.PHONY: all

all:
	escript -s echo.erl
	escript -s uniqueids.erl
	escript -s broadcast.erl

