# syntax=docker/dockerfile:1

ARG GO_VERSION=1.23.3

FROM golang:${GO_VERSION}-alpine AS build

ARG SERVICE
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations

RUN test -n "${SERVICE}" && \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /out/pulse \
      ./cmd/${SERVICE}

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S pulse && \
    adduser -S -G pulse pulse

COPY --from=build /out/pulse /usr/local/bin/pulse

USER pulse:pulse
ENTRYPOINT ["/usr/local/bin/pulse"]
