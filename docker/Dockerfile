# syntax=docker/dockerfile:1

ARG GO_VERSION=1.23.3

FROM golang:${GO_VERSION}-alpine AS build

ARG SERVICE
ARG TARGETOS=linux
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations

RUN test -n "${SERVICE}" && test -n "${TARGETARCH}" && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build \
      -mod=readonly \
      -trimpath \
      -buildvcs=false \
      -ldflags="-s -w -buildid=" \
      -o /out/pulse \
      ./cmd/${SERVICE}

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 10001 -S pulse && \
    adduser -u 10001 -S -G pulse pulse

COPY --from=build --chown=pulse:pulse --chmod=0555 /out/pulse /usr/local/bin/pulse

USER pulse:pulse
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/pulse"]
