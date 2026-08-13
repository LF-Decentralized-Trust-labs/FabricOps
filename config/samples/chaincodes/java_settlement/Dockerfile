FROM gradle:8.10-jdk21 AS build

WORKDIR /workspace

COPY build.gradle settings.gradle ./
COPY src ./src
RUN gradle --no-daemon clean shadowJar

FROM eclipse-temurin:21-jre
LABEL org.opencontainers.image.source="https://github.com/LF-Decentralized-Trust-labs/FabricOps" \
      org.opencontainers.image.description="FabricOps Java settlement CCaaS sample chaincode" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /chaincode
COPY --from=build /workspace/build/libs/chaincode.jar /chaincode/chaincode.jar

EXPOSE 7052
ENV CHAINCODE_SERVER_ADDRESS=0.0.0.0:7052

ENTRYPOINT ["java", "-jar", "/chaincode/chaincode.jar"]
