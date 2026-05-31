#!/usr/bin/env bash
# Build and test spark-lens-listener without a local Java/sbt install.
# Uses the official sbt Docker image.
set -euo pipefail

IMAGE="sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.10_7_1.9.9_2.12.19"

case "${1:-test}" in
  test)
    echo "==> Running tests (Scala 2.12 + 2.13)..."
    docker run --rm -v "$PWD":/build -w /build "$IMAGE" sbt "+test"
    ;;
  package)
    echo "==> Building JARs..."
    docker run --rm -v "$PWD":/build -w /build "$IMAGE" sbt "+package"
    echo "==> JARs written to target/"
    find target -name "*.jar" ! -path "*/sbt-*/*" | sort
    ;;
  publish-local)
    echo "==> Publishing to local Maven cache..."
    docker run --rm \
      -v "$PWD":/build \
      -v "$HOME/.ivy2":/root/.ivy2 \
      -v "$HOME/.sbt":/root/.sbt \
      -w /build "$IMAGE" sbt "+publishLocal"
    ;;
  *)
    echo "Usage: $0 [test|package|publish-local]"
    exit 1
    ;;
esac
