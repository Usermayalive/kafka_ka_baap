# Build: docker build -t stormlog .
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src src
RUN mvn -q package -DskipTests

FROM eclipse-temurin:25-jre
COPY --from=build /src/target/simple-kafka-1.0-SNAPSHOT.jar /app/stormlog.jar
VOLUME /data
# Args: <brokerId> <host> <port> <raftPort> [OS_PAGE_CACHE|PHYSICAL_FSYNC]
ENTRYPOINT ["java", "--enable-native-access=ALL-UNNAMED", "-Dstormlog.data.dir=/data", "-jar", "/app/stormlog.jar"]
