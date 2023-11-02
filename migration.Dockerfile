FROM alpine:3.18

ADD https://github.com/pressly/goose/releases/download/v3.15.0/goose_linux_x86_64 /bin/goose
RUN chmod +x /bin/goose

WORKDIR /migrations

ADD migrations/*.sql .

ENTRYPOINT ["/bin/goose"]
