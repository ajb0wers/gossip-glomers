# Build stage 0
FROM erlang:29

RUN apt-get update && apt-get -y install --no-install-recommends \
ed graphviz gnuplot curl default-jdk

# Set working directory
RUN mkdir /app
WORKDIR /app

ARG RELEASE="https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2"
RUN curl -SL "${RELEASE}" | tar -xjC /app

COPY erlang/ /app/erlang/
COPY Makefile .

# Expose relevant ports
EXPOSE 8080

CMD ["/bin/bash"]
