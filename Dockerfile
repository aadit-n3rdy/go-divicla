FROM golang:1.24.1-bookworm

RUN apt-get update && apt-get upgrade
RUN apt-get install -y python3  python3-torch

WORKDIR /app

COPY go.mod go.sum .
RUN go mod download && go mod verify

COPY . .

WORKDIR /app/compute
RUN go build .

WORKDIR /app/orchestrator
RUN go build .

WORKDIR /app/server
RUN go build .

WORKDIR /app/source
RUN go build .

WORKDIR /app
