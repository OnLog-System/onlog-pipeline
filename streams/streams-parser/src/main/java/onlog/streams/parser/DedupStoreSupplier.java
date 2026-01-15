package onlog.streams.parser;

import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

public class DedupStoreSupplier {

    public static final String STORE_NAME = "dedup-store";

    public static KeyValueBytesStoreSupplier supplier() {
        return Stores.persistentKeyValueStore(STORE_NAME);
    }

    private DedupStoreSupplier() {}
}
