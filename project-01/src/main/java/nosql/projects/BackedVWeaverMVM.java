package nosql.projects;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import java.util.AbstractMap.SimpleImmutableEntry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import nosql.projects.Material.KVStore;
import nosql.projects.Material.MultiVersionMap;
import nosql.projects.Material.Serializer;

public final class BackedVWeaverMVM<K extends Comparable<? super K>, P> implements MultiVersionMap<K, P> {

    
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class nodeRecord {
        public long ts;
        public String payload;
        public String nextKey;
        public String ridgyKey;
        public String kRidgyAbsolute;
        public int level;
        
        @SuppressWarnings("unused")
        public nodeRecord() {}
        
        public nodeRecord(long ts, String payload, String nextKey, String ridgyKey, int level, String kRidgyAbsolute) {
            this.ts = ts;
            this.payload = payload;
            this.nextKey = nextKey;
            this.kRidgyAbsolute = kRidgyAbsolute;
            this.ridgyKey = ridgyKey;
            this.level = level;
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Meta {
        public String headAbsolute;
        public long count;
        public List<String> lastAtLevel;
        
        public Meta() {}
        public Meta(String headAbsolute, long count, List<String> lastAtLevel) {
            this.headAbsolute = headAbsolute;
            this.count = count;
            this.lastAtLevel = lastAtLevel;
        }
    }
    
    private final class listHandle {
        private final K mapKey;
        private final String prefix;
        private final String META;
        private Meta meta;
        
        listHandle(K mapKey) {
            this.mapKey = mapKey;
            this.prefix = "VW:" + mapKey + ":";
            this.META = prefix + "__meta__";
            String m = store.get(META);
            
            if(m == null) {
                this.meta = new Meta(null, 0L, new ArrayList<>());
                persistMeta();
            } else {
                try {
                    this.meta = oMapper.readValue(m, Meta.class);
                    if (this.meta.lastAtLevel == null)
                    this.meta.lastAtLevel = new ArrayList<>();
                } catch (Exception exception) {
                    throw new RuntimeException("Corrupt meta", exception);
                }
            }
        }
        
        String absoluteFind(String localKey) {
            return (localKey == null || localKey.isEmpty()) ? null : prefix + localKey;
        }
        
        String localFind(String absoluteKey) {
            if (absoluteKey == null)
            return null;
            if(!absoluteKey.startsWith(prefix)) 
            throw new IllegalArgumentException("Wrong Prefix");
            return absoluteKey.substring(prefix.length());
        }
        
        String headAbs() {
            return meta.headAbsolute;
        }
        
        nodeRecord readAbsolute(String absKey) {
            try {
                if (absKey == null) 
                    return null;
                String json = store.get(absKey);
                if (json == null) {
                    if (VW_DEBUG)
                        System.err.println("[VW] readAbs missing key" + absKey);
                    return null;
                }
                return oMapper.readValue(json, nodeRecord.class);
            } catch (Exception exception) {
                throw new RuntimeException("Read Absolute failed" + absKey, exception);
            }
        }
        
        nodeRecord readLocal(String localKey) {
            return readAbsolute(absoluteFind(localKey));
        }
        
        void writeLocal(String localKey, nodeRecord record) {
            try {
                store.put(absoluteFind(localKey), oMapper.writeValueAsString(record));
            } catch (Exception exception) {
                throw new RuntimeException("Write local failed", exception);
            }
        }
        
        void persistMeta() {
            try {
                store.put(META, oMapper.writeValueAsString(meta));
            } catch (Exception exception) {
                throw new RuntimeException("Persist Meta failed", exception);
            }
        }
        
        int nextLevel() {
            long next = meta.count + 1;
            return Long.numberOfTrailingZeros(next);
        }
        
        String appendLocal(long ts, String payloadStr) {
            meta.count++;
            int level = Long.numberOfTrailingZeros(meta.count);
            
            while (meta.lastAtLevel.size() <= level) {
                meta.lastAtLevel.add(null);
            }
            
            String localKey = String.valueOf(ts);
            String prevHeadLocal = headAbs() == null ? null : localFind(headAbs());
            String ridgyLocal = meta.lastAtLevel.get(level);
            
            nodeRecord record = new nodeRecord(ts, payloadStr, prevHeadLocal, ridgyLocal, level, null);
            writeLocal(localKey, record);
            
            meta.headAbsolute = absoluteFind(localKey);
            meta.lastAtLevel.set(level, localKey);
            persistMeta();
            return localKey;
        }
        
        String findVisibleAbsolute(long t) {
            String currentAbs = headAbs();
            while (currentAbs != null) {
                nodeRecord current = readAbsolute(currentAbs);
                if (current.ts <= t)
                return currentAbs;
                if (current.ridgyKey != null) {
                    nodeRecord rec = readLocal(current.ridgyKey);
                    if (rec != null && rec.ts > t) {
                        currentAbs = absoluteFind(current.ridgyKey);
                        continue;
                    }
                }
                currentAbs = absoluteFind(current.nextKey);
            }
            return null;
        }
        
        String firstGEAbsolute(long t) {
            String currentAbsolute = headAbs();
            String lastNewerAbsolute = null;
            
            while(currentAbsolute != null) {
                nodeRecord current = readAbsolute(currentAbsolute);
                if (current == null) 
                    break;
                if (current.ts >= t) {
                    lastNewerAbsolute = currentAbsolute;
                    if (current.ridgyKey != null) {
                        nodeRecord rec = readLocal(current.ridgyKey);
                        if (rec != null && rec.ts >= t) {
                            currentAbsolute = absoluteFind(current.ridgyKey);
                        }
                    }
                    currentAbsolute = absoluteFind(current.nextKey);
                } else {
                    break;
                }
            }
            return lastNewerAbsolute;
        }
        
        String descendVisible(String startAbsolute, long t) {
            String currentAbsolute = (startAbsolute != null) ? startAbsolute : headAbs();
            while (currentAbsolute != null) {
                nodeRecord current = readAbsolute(currentAbsolute);
                if (current == null)
                    break;
                if (current.ts <= t) 
                    return currentAbsolute;
                if (current.ridgyKey != null) {
                    nodeRecord rec = readLocal(current.ridgyKey);
                    if (rec != null && rec.ts > t) {
                        currentAbsolute = absoluteFind(current.ridgyKey);
                    }
                }
                currentAbsolute = absoluteFind(current.nextKey);
            }
            return null;
        }
        
        void setKRidgyNext(listHandle nextList, String localKey, long v) {
            if (nextList == null)
                return;
            if (nextList.meta.headAbsolute == null)
                return;
            String targetAbsolute = nextList.firstGEAbsolute(v);
            if (targetAbsolute == null)
                return;
            nodeRecord record = readLocal(localKey);
            if (record == null)
                return;
            record.kRidgyAbsolute = targetAbsolute;
            writeLocal(localKey, record);
        }
    }
    
    private final ObjectMapper oMapper = new ObjectMapper();
    private final KVStore store;
    private final TreeMap<K, listHandle> trees = new TreeMap<>();
    private final Serializer<P> serializer;
    private long version = 1L;

    private static final boolean VW_DEBUG = false;

    public BackedVWeaverMVM(KVStore store, Serializer<P> serializer) {
        this.store = Objects.requireNonNull(store);
        this.serializer = Objects.requireNonNull(serializer);
    }

    private listHandle handle(K k) {
        return trees.computeIfAbsent(k, listHandle::new);
    }

    @Override
    public Map.Entry<K, P> get(K k, long t) {
        listHandle h = trees.get(k);
        if (h == null) 
            return null;
        String visibleAbs = h.findVisibleAbsolute(t);
        if (visibleAbs == null)
            return null;
        nodeRecord record = h.readAbsolute(visibleAbs);
        return new SimpleImmutableEntry<>(k, serializer.deSerialize(record.payload));
    }

    @Override
    public long append(K k, P p) {
        long v = version++;
        listHandle current = handle(k);

        String payloadStr = serializer.serialize(p);
        String localKey = current.appendLocal(v, payloadStr);

        Map.Entry<K, listHandle> nextEntry = trees.higherEntry(k);
        if (nextEntry != null) {
            current.setKRidgyNext(nextEntry.getValue(), localKey, v);
        }
        return v;
    }

    public Iterator<Map.Entry<K, P>> rangeSnapshot(K fromKey, boolean fromInc, K toKey, boolean toInc, long timeStamp) {
        if (trees.isEmpty())
            return Collections.<Map.Entry<K, P>> emptyList().iterator();

        NavigableMap<K, listHandle> subTree = trees.subMap(fromKey, fromInc, toKey, toInc);

        if (subTree.isEmpty())
            return Collections.<Map.Entry<K, P>> emptyList().iterator();
        
        List<Map.Entry<K, P>> outList = new ArrayList<>(subTree.size());

        String prevFirstGEAbsolute = null;

        boolean first = true;

        for (Map.Entry<K, listHandle> e : subTree.entrySet()) {
            listHandle h = e.getValue();
            String startAbs = null;

            if (first) {
                startAbs = null;
                first = false;
            } else if (prevFirstGEAbsolute != null) {
                nodeRecord prevNode = readAbsoluteGeneric(prevFirstGEAbsolute);
                if (prevNode != null && prevNode.kRidgyAbsolute != null) {
                    String expectedPrefix = "VW:" + e.getKey() + ":";
                    if (prevNode.kRidgyAbsolute.startsWith(expectedPrefix)) {
                        startAbs = prevNode.kRidgyAbsolute;
                    } else if (VW_DEBUG) {
                        System.err.println("[VW] kRidgy points to different list:" + prevNode.kRidgyAbsolute);
                    }
                    // startAbs = prevNode.kRidgyAbsolute.startsWith("VW:" + e.getKey() + ":") ? prevNode.kRidgyAbsolute : null;
                }
            }
            String visAbsolute = h.descendVisible(startAbs, timeStamp);
            if (visAbsolute != null) {
                nodeRecord visible = h.readAbsolute(visAbsolute);
                outList.add(new SimpleImmutableEntry<>(e.getKey(), serializer.deSerialize(visible.payload)));
            }
            prevFirstGEAbsolute = h.firstGEAbsolute(timeStamp);
        }
        return outList.iterator();
    }

    @Override
    public Iterator<Map.Entry<K, P>> snapshot(long timeStamp) {
        if (trees.isEmpty())
            return Collections.<Map.Entry<K, P>> emptyList().iterator();
        return rangeSnapshot(trees.firstKey(), true, trees.lastKey(), true, timeStamp);
    }

    private nodeRecord readAbsoluteGeneric(String absoluteKey) {
        try {
            String json = store.get(absoluteKey);
            if (json == null)
                return null;
            return oMapper.readValue(json, nodeRecord.class);
        } catch (Exception exception) {
            throw new RuntimeException("Read Generic failed", exception);
        }
    }
}
