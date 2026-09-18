FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml pom.xml
COPY common-avro common-avro
COPY order-processor order-processor
COPY order-producer/pom.xml order-producer/pom.xml
RUN mvn -pl order-processor -am clean package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/order-processor/target/order-processor-*.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]
