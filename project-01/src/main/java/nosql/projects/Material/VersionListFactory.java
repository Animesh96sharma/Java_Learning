package nosql.projects.Material;

public interface VersionListFactory<P> {
    VersionList<P> create(KVStore store, Serializer<P> serializer);
}
