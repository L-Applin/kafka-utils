package ca.applin.kafka.utils;

import lombok.Setter;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.Punctuator;

import java.util.Objects;
import java.util.function.Consumer;

public class CancellablePunctuator implements Punctuator {

    @Setter
    private Cancellable cancellable;
    private final Consumer<Long> work;

    public CancellablePunctuator(Consumer<Long> work) {
        this.work = Objects.requireNonNull(work,
            "Consumer instance must not be null");
    }

    @Override
    public void punctuate(long l) {
        work.accept(l);
        cancellable.cancel();
    }
}
