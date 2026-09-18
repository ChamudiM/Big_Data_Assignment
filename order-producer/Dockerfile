FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml pom.xml
COPY common-avro common-avro
COPY order-producer order-producer
COPY order-processor/pom.xml order-processor/pom.xml
RUN mvn -pl order-producer -am clean package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/order-producer/target/order-producer-*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
