package nosql.projects;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import nosql.projects.Material.KVStore;
import nosql.projects.Material.VersionList;
import nosql.projects.Material.*;

public final class BackedFrugalSkiplist<P> implements VersionList<P> {

    private static final String META_KEY = "__frugal_meta__";

    private static final class NodeRecord {
        public long ts;
        public String payload;
        public String nextKey;
        public String ridgyKey;

        @SuppressWarnings("unused")
        public NodeRecord() {}

        public NodeRecord(long ts, String payload, String nextKey, String ridgyKey) {
            this.ts = ts;
            this.payload = payload;
            this.nextKey = nextKey;
            this.ridgyKey = ridgyKey;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class Meta {
        public String headKey;
        public long count;
        public List<String> lastAtLevel;

        @SuppressWarnings("unused")
        public Meta() {}

        public Meta(String headKey, long count, List<String> lastAtLevel) {
            this.headKey = headKey;
            this.count = count;
            this.lastAtLevel = lastAtLevel;
        }
    }

    private final KVStore store;
    private final Serializer<P> serializer;
    private final ObjectMapper oMapper = new ObjectMapper();

    private Meta meta;

    public BackedFrugalSkiplist(KVStore store, Serializer<P> serializer) {
        this.store = store;
        this.serializer = serializer;
        String m = store.get(META_KEY);

        if (m == null) {
            this.meta = new Meta(null, 0L, new ArrayList<>());
            persistMeta();
        } else {
            try {
                this.meta = oMapper.readValue(m, Meta.class);
                if (this.meta.lastAtLevel ==  null) 
                    this.meta.lastAtLevel = new ArrayList<>();
            } catch (Exception exception) {
                throw new RuntimeException("Corrupt Meta", exception);
            }
        }
    }

    @Override
    public void append(P point, long timeStamp) {
        try {
            meta.count++;
            int level = Long.numberOfTrailingZeros(meta.count);

            while (meta.lastAtLevel.size() <= level) {
                meta.lastAtLevel.add(null);
            }

            final String newKey = String.valueOf(timeStamp);
            final String prevHead = meta.headKey;
            final String ridgy = meta.lastAtLevel.get(level);

            NodeRecord record = new NodeRecord(timeStamp, serializer.serialize(point), prevHead, ridgy);

            store.put(newKey, oMapper.writeValueAsString(record));
            meta.headKey = newKey;
            meta.lastAtLevel.set(level, newKey);
            persistMeta();
        } catch (Exception exception) {
            throw new RuntimeException("Append failed", exception);
        }
    }

    @Override
    public P findVisible(long t) {
        try {
            String key = meta.headKey;
            while (key != null && !key.isEmpty()) {
                NodeRecord record = readNode(key);
                if (record.ts <= t) 
                    return serializer.deSerialize(record.payload);

                if (record.ridgyKey != null) {
                    NodeRecord rec = readNode(record.ridgyKey);
                    if (rec.ts > t) {
                        key = rec.ridgyKey;
                        continue;
                    }
                }
                key = record.nextKey;
            }
            return null;
        } catch (Exception exception) {
            throw new RuntimeException("Find Visible failed", exception);
        }
    }

    private NodeRecord readNode(String key) {
        try {
            String json = store.get(key);
            if (json == null)
                throw new IllegalStateException("Missing Node" + key);
            return oMapper.readValue(json, NodeRecord.class);    
        } catch (Exception exception) {
            throw new RuntimeException("Corrupt node" + key, exception);
        }
    }

    private void persistMeta() {
        try {
            store.put(META_KEY, oMapper.writeValueAsString(meta));
        } catch (Exception exception) {
            throw new RuntimeException("Persist meta failed", exception);
        }
    }
}
