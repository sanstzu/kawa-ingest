FROM rust:alpine

WORKDIR /usr/src/app

COPY . .

RUN ["apk", "add" , "build-base", "protobuf", "protobuf-dev", "protoc"]

RUN ["cargo", "build", "--release"]

EXPOSE 1935

CMD ["cargo", "run", "--release"]