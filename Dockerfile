# Build stage 0
FROM erlang:27

#Set working directory
RUN mkdir /apps
WORKDIR /apps
COPY echo.erl .

WORKDIR /
RUN apt-get update && \
    apt-get -y install graphviz gnuplot wget default-jdk

RUN wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2
RUN tar -xf maelstrom.tar.bz2

WORKDIR /maelstrom

# Expose relevant ports
EXPOSE 8080

CMD ["/bin/bash"]
