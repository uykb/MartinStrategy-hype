# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build (pure Go, no CGO needed)
RUN CGO_ENABLED=0 GOOS=linux go build -o bot cmd/bot/main.go

# Runtime Stage
FROM alpine:3.18

WORKDIR /app

RUN apk --no-cache add ca-certificates

COPY --from=builder /app/bot .
COPY --from=builder /app/config.yaml.example ./config.yaml.example

# config.yaml 通过 docker-compose volume 挂载，不打包进镜像
# 首次部署：cp config.yaml.example config.yaml && 编辑填入密钥

EXPOSE 8080

CMD ["./bot"]