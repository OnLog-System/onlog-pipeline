package onlog.streams.parser;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class DedupTransformer
        implements Processor<String, ParsedWrapper, String, ParsedWrapper> {

    private KeyValueStore<String, Long> store;
    private ProcessorContext<String, ParsedWrapper> context;

    // @Override
    // public void init(ProcessorContext<String, ParsedWrapper> context) {
    //     this.context = context;
    //     this.store = context.getStateStore(DedupStoreSupplier.STORE_NAME);
    // }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext<String, ParsedWrapper> context) {
        this.context = context;
        this.store =
            (KeyValueStore<String, Long>)
                context.getStateStore(DedupStoreSupplier.STORE_NAME);
    }

    @Override
    public void process(Record<String, ParsedWrapper> record) {
        ParsedWrapper v = record.value();

        if (v == null || v.devEui == null || v.fCnt == null) {
            context.forward(record);
            return;
        }

        if (v.edgeIngestTime == null) {
            context.forward(record);
            return;
        }

        String key = v.devEui + ":" + v.fCnt;
        long now = v.edgeIngestTime.toEpochMilli();
        long ttl = ParserConfig.DEDUP_TTL.toMillis();

        Long last = store.get(key);

        // 중복이 아니면 통과
        if (last == null) {
            store.put(key, now);
            context.forward(record);
            return;
        }

        // TTL 초과 → 새로운 이벤트로 간주 + 기존 key 정리
        if (now - last > ttl) {
            store.put(key, now);   // overwrite
            context.forward(record);
        }

        // TTL 이내 → duplicate → drop
    }
}