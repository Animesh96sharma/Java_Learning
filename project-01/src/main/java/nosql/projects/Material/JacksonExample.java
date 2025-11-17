package nosql.projects.Material;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonExample {

    public static void main(String[] args) {

        ObjectMapper mapper = new ObjectMapper();
        ExampleClass exampleObject = new ExampleClass(1, 2, "three");
        String asJson;
        try {
            asJson = mapper.writeValueAsString(exampleObject);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        System.out.println(asJson);
        ExampleClass deserialized;
        try {
            deserialized = mapper.readValue(asJson, ExampleClass.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        System.out.println(deserialized.foo + ", " + deserialized.bar + ", " + deserialized.baz);

    }


    public static class ExampleClass {


        int foo;
        public long bar;
        String baz;

        // Default constructor needed for ObjectMapper
        public ExampleClass(){};
        // Getter for non-public attributes needed for ObjectMapper
        public String getBaz() {return baz;}
        public int getFoo() {return foo;}

        public ExampleClass(int foo, long bar, String baz) {
            this.foo = foo;
            this.bar = bar;
            this.baz = baz;
        }
    }
}
