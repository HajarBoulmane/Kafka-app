package org.example.kafkaspringcloudstream.handlers;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.example.kafkaspringcloudstream.events.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class PageEventHandler {

    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return input -> {
            System.out.println("*********************");
            System.out.println(input.toString());
            System.out.println("*********************");
        };
    }

    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> new PageEvent(
                Math.random() > 0.5 ? "P1" : "P2",
                Math.random() > 0.5 ? "U1" : "U2",
                new Date(),
                10 + new Random().nextInt(10000)
        );
    }

    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamFunction() {
        return input -> input
                // 1. ne garder que les visites > 100 ms
                .filter((k, v) -> v.duration() > 100)
                // 2. nouvelle clé = nom de la page, valeur = 0 (on veut juste compter)
                .map((k, v) -> new KeyValue<>(v.name(), 0L))
                // 3. grouper par clé (le nom de la page)
                .groupBy((k, v) -> k, Grouped.with(Serdes.String(), Serdes.Long()))
                // 4. fenêtre glissante de 5 secondes
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
                // 5. compter, résultat stocké dans une vue matérialisée "page-count"
                .count(Materialized.as("page-count"))
                // 6. KTable -> KStream
                .toStream()
                // 7. la clé est une Windowed<String>, on garde seulement le nom de la page
                .map((k, v) -> new KeyValue<>(k.key(), v));
    }
}