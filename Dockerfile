# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build (pure Go, no CGO needed)
RUN CGO_ENABLED=0 GOOS=linux go build -o bot ./cmd/bot/

# Runtime Stage
FROM alpine:3.18

WORKDIR /app

RUN apk --no-cache add ca-certificates

COPY --from=builder /app/bot .

# 健康检查端口
EXPOSE 8080

# 所有配置通过环境变量注入，无需 config.yaml
# 必需：MARTIN_EXCHANGE_API_KEY, MARTIN_EXCHANGE_API_SECRET
# 可选：MARTIN_EXCHANGE_SYMBOL (默认 HYPE), MARTIN_LOG_LEVEL (默认 info) 等
CMD ["./bot"]