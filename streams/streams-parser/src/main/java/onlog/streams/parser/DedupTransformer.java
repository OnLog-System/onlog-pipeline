package onlog.streams.parser;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class DedupTransformer
        implements Processor<String, ParsedWrapper, String, ParsedWrapper> {

    private KeyValueStore<String, Long> store;
    private ProcessorContext<String, ParsedWrapper> context;

    @Override
    public void init(ProcessorContext<String, ParsedWrapper> context) {
        this.context = context;
        this.store = context.getStateStore(DedupStoreSupplier.STORE_NAME);
    }

    @Override
    public void process(Record<String, ParsedWrapper> record) {
        ParsedWrapper v = record.value();

        if (v == null || v.devEui == null || v.fCnt == null) {
            context.forward(record);
            return;
        }

        String key = v.devEui + ":" + v.fCnt;
        long now = v.edgeIngestTime.toEpochMilli();

        Long last = store.get(key);
        if (last == null || now - last > ParserConfig.DEDUP_TTL.toMillis()) {
            store.put(key, now);
            context.forward(record);
        }
    }
}
