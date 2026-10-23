package org.example.kafkaspringcloudstream.controllers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.example.kafkaspringcloudstream.events.PageEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class PageEventController {

    @Autowired
    private StreamBridge streamBridge;

    @GetMapping("/publish")
    public PageEvent publish(@RequestParam String name, @RequestParam String topic) {
        PageEvent pageEvent = new PageEvent(
                name,
                Math.random() > 0.5 ? "U1" : "U2",   // 50% U1, 50% U2
                new Date(),                           // date système
                10 + new Random().nextInt(10000)      // durée aléatoire
        );
        streamBridge.send(topic, pageEvent);          // publie dans le topic
        return pageEvent;                             // renvoie l'événement au navigateur
    }

    @Autowired
    private InteractiveQueryService interactiveQueryService;

    @GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {
        return Flux.interval(Duration.ofSeconds(1))      // un tick chaque seconde
                .map(sequence -> {
                    Map<String, Long> map = new HashMap<>();

                    ReadOnlyWindowStore<String, Long> windowStore =
                            interactiveQueryService.getQueryableStore("page-count",
                                    QueryableStoreTypes.windowStore());

                    Instant now = Instant.now();
                    Instant from = now.minusMillis(5000);   // 5 secondes en MILLIsecondes !

                    try (KeyValueIterator<Windowed<String>, Long> it = windowStore.fetchAll(from, now)) {
                        while (it.hasNext()) {
                            KeyValue<Windowed<String>, Long> next = it.next();
                            map.put(next.key.key(), next.value);   // ex: "P1" -> 23
                        }
                    }
                    return map;
                });
    }
}