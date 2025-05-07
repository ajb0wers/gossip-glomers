# Build stage 0
FROM erlang:27

RUN apt-get update && \
    apt-get -y install --no-install-recommends graphviz gnuplot wget default-jdk

#Set working directory
RUN mkdir /app
WORKDIR /app

RUN wget --progress=dot:mega \
    https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2 && \
    tar -xf maelstrom.tar.bz2  && \
    rm maelstrom.tar.bz2

RUN mkdir bin
COPY echo uniqueids broadcast bin/ 

WORKDIR /app/maelstrom

# Expose relevant ports
EXPOSE 8080

CMD ["/bin/bash"]
