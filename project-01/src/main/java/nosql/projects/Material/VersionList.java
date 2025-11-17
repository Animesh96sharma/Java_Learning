package nosql.projects.Material;

public interface VersionList<P> {
    /**
     * Appends a new payload p with corresponding version 'timestamp' to the list.
     * We assume the timestamps are always increasing for consecutive calls to append.
     * @param p
     * @param timestamp
     */
    void append(P p, long timestamp);

    /**
     * Finds the visible payload at time 'timestamp'.
     * @param timestamp
     * @return The newest payload that has an assigned timestamp/version that
     * is smaller or equal to the given 'timestamp'
     */
    P findVisible(long timestamp);
}
