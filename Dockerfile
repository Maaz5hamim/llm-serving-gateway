FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN go test ./... && CGO_ENABLED=0 go build -o /gateway ./cmd/gateway && CGO_ENABLED=0 go build -o /mockprovider ./cmd/mockprovider
FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /gateway /mockprovider /usr/local/bin/
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["gateway"]
