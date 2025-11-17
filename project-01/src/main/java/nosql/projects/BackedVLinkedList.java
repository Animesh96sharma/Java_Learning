package nosql.projects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import nosql.projects.Material.KVStore;
import nosql.projects.Material.Serializer;
import nosql.projects.Material.VersionList;

public final class BackedVLinkedList<P> implements VersionList<P> {
    private static final String HEAD_KEY = "__head__" ;
    private final KVStore store;
    private final Serializer<P> serializer;
    private final ObjectMapper mapper = new ObjectMapper();

    @JsonIgnoreProperties(ignoreUnknown = true) 
    private static final class NodeRecord {
        public long ts;
        public String payload;
        public String nextKey;

        @SuppressWarnings("unused")
        public NodeRecord() {}

        public NodeRecord(long ts, String payload, String nextKey) {
            this.ts = ts;
            this.payload = payload;
            this.nextKey = nextKey;
        }
    }

    public BackedVLinkedList(KVStore store, Serializer<P> serializer) {
        this.store = store;
        this.serializer = serializer;
        if (store.get(HEAD_KEY) == null) {
            store.put(HEAD_KEY, "");
        }
    }

    @Override
    public void append(P p, long timeStamp) {
        try {
            final String newKey = String.valueOf(timeStamp);
            final String prevHeadKey = headKeyOrNull();

            final String payloadStr = serializer.serialize(p);
            NodeRecord record = new NodeRecord(timeStamp, payloadStr, prevHeadKey);
            
            store.put(newKey, mapper.writeValueAsString(record));
            store.put(HEAD_KEY, newKey);
        } catch (Exception exception) {
            throw new RuntimeException("Append failed", exception);
        }
    }

    @Override
    public P findVisible(long timeStamp) {
        try {
            String key = headKeyOrNull();
            while (key != null && !key.isEmpty()) {
                NodeRecord record = readNode(key);
                if (record.ts <= timeStamp) {
                    return serializer.deSerialize(record.payload);
                }
                key = record.nextKey;
            }
            return null;
        } catch (Exception exception) {
            throw new RuntimeException("Find visible function failed", exception);
        }
    }

    private NodeRecord readNode(String key) {
        try {
            String json = store.get(key);
            if (json == null) {
                throw new IllegalStateException("Missing node");
            }
            return mapper.readValue(json, NodeRecord.class);
        } catch (Exception exception) {
            throw new RuntimeException("Corrupt node", exception);
        }
    }

    private String headKeyOrNull() {
        String v = store.get(HEAD_KEY);
        return (v == null || v.isEmpty() ? null : v);
    }
}
