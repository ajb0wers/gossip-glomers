# Build stage 0
FROM erlang:27

#Set working directory
RUN mkdir /apps
WORKDIR /apps
COPY echo.erl .

# RUN apk add --no-cache openssl && \
#     apk add --no-cache ncurses-libs && \
#     apk add --no-cache libstdc++ && \
#     apk add --no-cache libgcc && \
#     apk add --no-cache bash
# 
# 
# WORKDIR /
# RUN apk add --no-cache graphviz  && \
#     apk add --no-cache gnuplot
# 
RUN wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2
RUN tar -xf maelstrom.tar.bz2
WORKDIR /maelstrom

# Expose relevant ports
EXPOSE 8080

CMD ["/bin/sh"]
