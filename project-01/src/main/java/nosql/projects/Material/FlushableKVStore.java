package nosql.projects.Material;

public interface FlushableKVStore extends KVStore {
    void flushDB();
}
