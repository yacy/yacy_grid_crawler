## yacy_grid_crawler dockerfile
## examples:
# docker build -t yacy_grid_crawler .
# docker run -d --rm -p 8300:8300 --name yacy_grid_crawler yacy_grid_crawler
## Check if the service is running:
# curl http://localhost:8300/yacy/grid/mcp/info/status.json

# build app
FROM adoptopenjdk/openjdk8:alpine AS appbuilder
COPY ./ /app
WORKDIR /app
RUN ./gradlew assemble

# build dist
FROM adoptopenjdk/openjdk8:alpine
LABEL maintainer="Michael Peter Christen <mc@yacy.net>"
ENV DEBIAN_FRONTEND noninteractive
ARG default_branch=master
COPY ./conf /app/conf/
COPY --from=appbuilder /app/build/libs/ ./app/build/libs/
WORKDIR /app
EXPOSE 8300
CMD ["java", "-jar", "/app/build/libs/yacy_grid_crawler-0.0.1-SNAPSHOT-all.jar"]
