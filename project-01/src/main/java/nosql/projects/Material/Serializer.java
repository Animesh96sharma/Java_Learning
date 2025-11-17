package nosql.projects.Material;


public interface Serializer<T> {

    String serialize(T t);
    T deSerialize(String serializedT);
}
