FROM golang:1.21.5

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /user-server-docker

EXPOSE 8080

CMD [ "/user-server-docker" ]