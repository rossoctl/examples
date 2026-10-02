FROM public.ecr.aws/docker/library/golang:1.27.1-bookworm

WORKDIR /app
EXPOSE 8080

COPY . /app
RUN ls -l /app

RUN CGO_ENABLED=0 GOOS=linux go build -o /mcp-mitm /app/cli/main/main.go

CMD ["/mcp-mitm"]
