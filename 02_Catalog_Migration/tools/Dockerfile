FROM alpine:3.20
RUN apk add --no-cache curl jq ca-certificates \
 && curl -fsSL "https://dl.min.io/client/mc/release/linux-amd64/archive/mc.RELEASE.2025-08-13T08-35-41Z" \
    -o /usr/local/bin/mc \
 && chmod +x /usr/local/bin/mc
