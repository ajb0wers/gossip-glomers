# Build stage 0
FROM erlang:27

RUN apt-get update && apt-get -y install --no-install-recommends \
ed graphviz gnuplot wget default-jdk

#Set working directory
RUN mkdir /app
WORKDIR /app

# RUN wget --no-verbose \
# https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2 \
# && tar -xf maelstrom.tar.bz2 \
# && rm maelstrom.tar.bz2
ADD https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2 \
    maelstrom.tar.bz2
RUN tar -xf maelstrom.tar.bz2 && rm maelstrom.tar.bz2

COPY erlang/ /app/erlang/
COPY Makefile .

# Expose relevant ports
EXPOSE 8080

CMD ["/bin/bash"]
