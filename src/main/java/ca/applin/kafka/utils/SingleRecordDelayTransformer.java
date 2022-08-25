package ca.applin.kafka.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;

import java.time.Duration;

@Slf4j
public class SingleRecordDelayTransformer<K, V> implements Transformer<K, V, KeyValue<K, V>> {

    private ProcessorContext ctx;
    private final Duration duration;

    public SingleRecordDelayTransformer(Duration duration) {
        this.duration = duration;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        log.info("INIT CONTEXT");
       this.ctx = processorContext;
    }

    @Override
    public KeyValue<K, V> transform(K key, V value) {
        CancellablePunctuator work = new CancellablePunctuator(time -> {
            ctx.forward(key, value, To.all().withTimestamp(time));
            ctx.commit();
        });
        Cancellable c = ctx.schedule(duration, PunctuationType.WALL_CLOCK_TIME, work);
        work.setCancellable(c);
        // ok to return null
        return null;
    }

    @Override
    public void close() {
        // do nothing
    }
}
