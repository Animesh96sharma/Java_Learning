package nosql.projects;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import nosql.projects.Material.KVStore;
import nosql.projects.Material.MultiVersionMap;
import nosql.projects.Material.Serializer;
import nosql.projects.Material.VersionList;
import nosql.projects.Material.VersionListFactory;

public final class BackedSimpleMVM<K extends Comparable<? super K>, P> implements MultiVersionMap<K, P> {
    private final TreeMap<K, VersionList<P>> index = new TreeMap<>();
    private final VersionListFactory<P> factory;
    private final KVStore store;
    private final Serializer<P> serializer;

    private long version = 1L;

    public BackedSimpleMVM(VersionListFactory<P> factory, KVStore store, Serializer<P> serializer) {
        this.factory = Objects.requireNonNull(factory);
        this.store = Objects.requireNonNull(store);
        this.serializer = Objects.requireNonNull(serializer);
    }

    @Override
    public Map.Entry<K, P> get(K k, long t) {
        VersionList<P> vl = index.get(k);
        if (vl == null)
            return null;
        
            P vis = vl.findVisible(t);
            return (vis == null) ? null : new SimpleImmutableEntry<>(k, vis);
    }

    @Override
    public long append(K k, P p) {
        VersionList<P> vl = index.get(k);
        if (vl == null) {
            vl = factory.create(store, serializer);
            index.put(k, vl);
        }
        long assigned = version++;
        vl.append(p, assigned);
        return assigned;
    }

    @Override
    public Iterator<Map.Entry<K, P>> rangeSnapshot(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, long timeStamp) {
        if (index.isEmpty())
            return Collections.<Map.Entry<K, P>> emptyList().iterator();
        NavigableMap<K, VersionList<P>> sub = index.subMap(fromKey, fromInclusive, toKey, toInclusive);

        if (sub.isEmpty())
            return Collections.<Map.Entry<K, P>> emptyList().iterator();
        
        List<Map.Entry<K, P>> out = new ArrayList<>(sub.size());
        for (Map.Entry<K, VersionList<P>> e : sub.entrySet()) {
            P vis = e.getValue().findVisible(timeStamp);
            if (vis != null)
                out.add(new SimpleImmutableEntry<>(e.getKey(), vis));
        }
        return out.iterator();
    }

    @Override
    public Iterator<Map.Entry<K, P>> snapshot(long timeStamp) {
        if (index.isEmpty()) 
            return Collections.<Map.Entry<K, P>> emptyList().iterator();
        
        K firstKey = index.firstKey();
        K lastKey = index.lastKey();

        return rangeSnapshot(firstKey, true, lastKey, true, timeStamp);
    }
}