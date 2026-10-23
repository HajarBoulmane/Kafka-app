# Kafka Spring Cloud Stream: Real-Time Page Analytics

A small event-driven project that simulates page visits, processes them in real time with Kafka Streams, and displays the live result in a browser chart.

Built with Spring Boot, Spring Cloud Stream, Apache Kafka, Kafka Streams and Smoothie.js.

---

## Architecture

```
 MAIN CIRCUIT (real time)

 Supplier --> T3 --> Kafka Streams --> T4
 (fake visits)       (filter, group,
                      5s window, count)
                           |
                           v
                     store "page-count"
                           |
                           v
              GET /analytics (SSE, every 1s)
                           |
                           v
                    index.html (live chart)


 TEST CIRCUIT (manual)

 browser --> GET /publish --> T2 --> Consumer --> console
```

### Topics

| Topic | Written by | Read by |
|-------|-----------|---------|
| T2 | `/publish` endpoint (manual) | `pageEventConsumer` |
| T3 | `pageEventSupplier` (automatic) | `kStreamFunction` |
| T4 | `kStreamFunction` | console consumer (for checking) |

---

## Project structure

```
src/main/java/org/example/kafkaspringcloudstream
├── KafkaSpringCloudStreamApplication.java   # generated, no code to add
├── controllers
│   └── PageEventController.java             # /publish and /analytics
├── events
│   └── PageEvent.java                       # record: name, user, date, duration
└── handlers
    └── PageEventHandler.java                # Consumer, Supplier, Kafka Streams

src/main/resources
├── application.properties                   # wires functions to topics
└── static/index.html                        # live chart page

docker-compose.yml                           # Zookeeper + Kafka broker
```

---

## Components

**PageEvent**: a record describing one visit: page name, user, date, duration in ms.

**PageEventHandler** contains three Spring beans:
- `pageEventConsumer` (`Consumer`): reads T2 and prints events to the console.
- `pageEventSupplier` (`Supplier`): called automatically by Spring on a timer, creates a random PageEvent and publishes it to T3.
- `kStreamFunction` (`Function<KStream, KStream>`): reads T3, filters visits with duration > 100 ms, groups by page name, counts per 5-second window, stores the result in `page-count`, and writes it to T4.

**PageEventController**:
- `GET /publish?name=P1&topic=T2`: publishes one PageEvent manually through `StreamBridge`.
- `GET /analytics`: Server-Sent Events stream. Every second it reads the `page-count` store (last 5 seconds) and pushes `{"P1": n, "P2": m}` to the browser.

**index.html**: opens an `EventSource("/analytics")` and draws one curve per page with Smoothie.js (green = P1, red = P2).

---

## Prerequisites

- Java 21
- Maven (or the included `mvnw`)
- Docker and Docker Compose
- IntelliJ IDEA (optional)

## Configuration

`src/main/resources/application.properties`:

```properties
server.port=8080

spring.cloud.function.definition=pageEventConsumer;pageEventSupplier;kStreamFunction

spring.cloud.stream.bindings.pageEventConsumer-in-0.destination=T2

spring.cloud.stream.bindings.pageEventSupplier-out-0.destination=T3
spring.cloud.stream.poller.fixed-delay=200

spring.cloud.stream.bindings.kStreamFunction-in-0.destination=T3
spring.cloud.stream.bindings.kStreamFunction-out-0.destination=T4

spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000
```

Binding names follow `functionName-in-0` / `functionName-out-0`. A function not listed in `spring.cloud.function.definition` is ignored.

---

## Run

1. Start Kafka:
   ```bash
   docker compose up -d
   docker ps
   ```
2. Run `KafkaSpringCloudStreamApplication` from IntelliJ (or `./mvnw spring-boot:run`).
3. Open `http://localhost:8080/index.html`.
4. Wait about 30 seconds (see Notes). The curves then start moving.

### Manual test of the Consumer

```
http://localhost:8080/publish?name=P1&topic=T2
```

The event is printed in the IntelliJ console.

---

## Checking topics from the console

Replace `bdcc-kafka-broker` with your container name.

Read a topic:
```bash
docker exec -it bdcc-kafka-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic T3
```

Read T4 (key is a String, value is a Long):
```bash
docker exec -it bdcc-kafka-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic T4 \
  --property print.key=true --property print.value=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

Add `--from-beginning` to replay all messages in a topic.

---

## Notes and common pitfalls

- **30-second startup delay**: Kafka Streams publishes its first results after its default commit interval. Until then the store is empty and the chart is flat. `commit.interval.ms=1000` speeds up later updates, but the first result still takes time.
- **Window size**: use `minusMillis(5000)` for 5 seconds. Using 5000 seconds means nothing shows for a very long time.
- **Consumer vs chart**: the Consumer reads T2, not T3, so the Supplier events never appear in the console. Only events sent through `/publish` do.
- **Function ignored**: check it is in `spring.cloud.function.definition` and has a binding in the properties.
- **Container name**: the name in `docker exec` must match `container_name` in `docker-compose.yml`.
- **Package scanning**: `controllers`, `events` and `handlers` must stay under the main class package, otherwise Spring will not find them.

---

## Possible extensions

- Replace `count` with `aggregate` to compute the total or average visit duration per page.
- Remove the `filter` to count every visit.
- Change the window size or the Supplier rate.
- Add more pages or users to the generated events.
