package nosql.projects.Material;


import java.util.Iterator;
import java.util.Map;

public interface MultiVersionMap<K extends Comparable<? super K>, P> {
    /**
     * Gets the visible version at timestamp t of the data corresponding to key k.
     * @param k key
     * @param t timestamp
     * @return
     */
    Map.Entry<K, P> get(K k, long t);

    /**
     * Appends the new (possibly updated) payload p corresponding to key k to the MultiVersionMap
     * @param k key
     * @param p payload
     * @return the version, that was internally (by the MultiVersionMap) assigned to the new data item
     */
    long append(K k, P p);

    /**
     * A Range-Snapshot.
     * @param fromKey
     * @param fromInclusive
     * @param toKey
     * @param toInclusive
     * @param timestamp
     * @return The visible version at time timestamp for all records whose key lies between
     * fromKey (inclusive, iff fromInclusive) and toKey (inclusive iff toInclusive), in key-order.
     */
    Iterator<Map.Entry<K,P>> rangeSnapshot(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, long timestamp);

    /**
     * A full-range snapshot for time timestamp.
     * @param timestamp
     * @return
     */
    Iterator<Map.Entry<K, P>> snapshot(long timestamp);
}
