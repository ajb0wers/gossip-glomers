# Build stage 0
FROM erlang:27

RUN \
apt-get update \
&& apt-get -y install --no-install-recommends \
ed graphviz gnuplot wget default-jdk

#Set working directory
RUN mkdir /app
WORKDIR /app

RUN \
wget --no-verbose \
https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2 \
&& tar -xf maelstrom.tar.bz2 \
&& rm maelstrom.tar.bz2

# RUN mkdir bin
# 1/echo.erl 2/uniqueids.erl 3e/broadcast.erl
COPY 1/ 1/
COPY 2/ 2/
COPY 3/ 3/
COPY 3e/ 3e/
COPY 4/ 4/
COPY Makefile .

# Expose relevant ports
EXPOSE 8080

CMD ["/bin/bash"]
