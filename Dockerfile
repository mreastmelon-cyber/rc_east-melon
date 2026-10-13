FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/notification-service ./cmd/notification-service && \
    CGO_ENABLED=0 go build -trimpath -o /out/mock-provider ./cmd/mock-provider

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/ /app/
COPY configs/ /app/configs/
WORKDIR /app
USER 65532:65532
EXPOSE 50051 9090
ENTRYPOINT ["/app/notification-service"]
