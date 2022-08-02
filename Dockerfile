FROM golang:1.18

WORKDIR /go/src
ENV PATH="/go/bin:${PATH}"

RUN apt-get update && \
    apt-get install build-essential librdkafka-dev -y

COPY . .

CMD ["tail", "-f", "/dev/null"]