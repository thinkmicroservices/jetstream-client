
Run NATS JetStream in Docker

docker run --network host -p 4222:4222 nats -js



Build JetStream REST Client Jar file

mvn clean package



Run JetStream REST Client Jar file

java -jar ./target/jetstream-client-0.0.1-SNAPSHOT.jar 



Build Spring Native GraalVM native executable

mvn spring-boot:build-image



Run the JetStream REST Client native executable

docker run --network host --rm -p 8080:8080 jetstream-client:0.0.1-SNAPSHOT