## Kinesis Stream Reader
More info coming soon.

## Docker Commands
```docker build -t kinesis-reader --build-arg VERSION=$MY_COMPONENT_VERSION -f Dockerfile.production .
docker build -t kinesis-reader-test -f Dockerfile.test .
docker run --rm kinesis-reader-test
# https://dzone.com/articles/testing-nodejs-application-using-mocha-and-docker

docker run -p 12345:3000 -d kinesis-reader
# https://nodejs.org/en/docs/guides/nodejs-docker-webapp/```