package nosql.projects;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import redis.clients.jedis.Jedis;
import com.fasterxml.jackson.databind.ObjectMapper;

import nosql.projects.Material.KVStore;
import nosql.projects.Material.MultiVersionMap;
import nosql.projects.Material.Serializer;
import nosql.projects.Material.VersionListFactory;

public class Test {
    public record Payload(String title, String comment, String timestamp){};
    public interface FlushableKVStore extends KVStore {
        void flushDB();
    }

    public static final class jedisKV implements FlushableKVStore {
        private final Jedis jedis;
        private final String ns;
        public jedisKV(String host, int port, String nameSpace) {
            this.jedis = new Jedis(host, port);
            this.ns = (nameSpace == null || nameSpace.isEmpty()) ? "" :(nameSpace + ":");
        }
        private String k(String k) {
            return ns + k;
        }

        @Override
        public void put(String storeKey, String storeValue) {
            jedis.set(k(storeKey), storeValue);
        }

        @Override
        public String get(String storeKey) {
            return jedis.get(k(storeKey));
        }

        @Override
        public void flushDB() {
            jedis.flushDB();
        }

        public void close() {
            jedis.close();
        }
    }

    public static final class jacksonSerialiser<P> implements Serializer<P> {
        private final ObjectMapper oMapper = new ObjectMapper();
        private final Class<P> type;
        public jacksonSerialiser(Class<P> type) {
            this.type = type;
        }

        @Override
        public String serialize(P temp) {
            try {
                return oMapper.writeValueAsString(temp);
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }

        @Override
        public P deSerialize(String serialized) {
            try {
                return oMapper.readValue(serialized, type);
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    public static List<Map.Entry<String, Payload>> readData(String path) {
        List<Map.Entry<String, Payload>> l = new ArrayList<>();
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            // Skip header
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",", 4); // key,title,comment,"version"
                String key = values[0];
                String title = values[1];
                String comment = values[2];
                String timestamp = values[3];

                l.add(new AbstractMap.SimpleEntry<>(key, new Payload(title, comment, timestamp)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return l;
    }

    private static long nowTime() {
        return System.nanoTime();
    }

    private static double milliSeconds(long nanoSeconds) {
        return nanoSeconds / 1_000_000.0;
    }
    
    private static double averageCalc(List<Double> xs) {
        return xs.stream().mapToDouble(d -> d).average().orElse(0.0);
    }

    public static void main(String[] args) {

        Path csvPath = Path.of("src", "main", "java", "nosql", "projects", "Data", "test_data.csv");
        String csvPathStr = csvPath.toString();
        
        String homePath = "127.0.0.1";
        int portNumber = 6379;

        List<Map.Entry<String, Payload>> rowList = readData(csvPathStr);

        if (rowList.isEmpty()) {
            System.out.println("No Rows loaded");
            return;
        }

        Serializer<Payload> serializerMain = new jacksonSerialiser<>(Payload.class);

        jedisKV kvLL = new jedisKV(homePath, portNumber, "LL");
        jedisKV kvFSL = new jedisKV(homePath, portNumber, "FSL");
        jedisKV kvVW = new jedisKV(homePath, portNumber, "VW");

        try {
            VersionListFactory<Payload> llFactory = (store, serializer) -> new BackedVLinkedList<>(store, serializer);
            VersionListFactory<Payload> fslFactory = (store, serializer) -> new BackedFrugalSkiplist<>(store, serializer);

            kvLL.flushDB();
            MultiVersionMap<String, Payload> mvmLL = new BackedSimpleMVM<>(llFactory, kvLL, serializerMain);
            for (var e : rowList) {
                long assigned = mvmLL.append(e.getKey(), e.getValue());
                String csvTime = e.getValue().timestamp();

                if (csvTime != null && !csvTime.isBlank()) {
                    String normalised = csvTime.replaceAll("[^0-9]", "");
                    if (!normalised.isEmpty() && assigned != Long.parseLong(normalised)) {
                        throw new IllegalStateException("Timestamp mismatch for key " + e.getKey());
                    }
                }
            }
            System.out.println("LL when t = 20, range [KEY002, KEY004]:");
            printRange(mvmLL, "KEY002", true, "KEY004", true, 20L);
            
            kvFSL.flushDB();
            MultiVersionMap<String, Payload> mvmFSL = new BackedSimpleMVM<>(fslFactory, kvFSL, serializerMain);
            System.out.println("Frugal Skiplist when t = 20, range [KEY002, KEY004]:");
            printRange(mvmFSL, "KEY002", true, "KEY004", true, 20L);
            
            long[] probes = {10, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000};
            int runs = 5;
            
            kvVW.flushDB();
            MultiVersionMap<String, Test.Payload> vWeaver = new BackedVWeaverMVM<>(kvVW, serializerMain);
            for (var e : rowList) {
                vWeaver.append(e.getKey(), e.getValue());
            }
            System.out.println("VWeaver when t = 20, range [KEY002, KEY004]:");
            printRange(vWeaver, "KEY002", true, "KEY004", true, 20L);
            
            benchmarkVariant("BackedVLinkedList", rowList, probes, runs, llFactory, kvLL, serializerMain);
            benchmarkVariant("BackedFrugalSkipList", rowList, probes, runs, fslFactory, kvFSL, serializerMain);
            benchmarkVWeaver("BackedVWeaverMVM", rowList, probes, runs, kvVW, serializerMain);

        } catch (Exception exception) {
            throw new RuntimeException(exception);
        } finally {
            kvFSL.close();
            kvLL.close();
            kvVW.close();
        }
        
        /* -- Test -- */
        /* You can use the provided method to read the data like
        List<Map.Entry<String, Payload>> data = readData("path/to/test_data.csv");

        Correct output for range ["KEY002", "KEY004"] and timestamp 20 is
        KEY002=Payload[title=Some Title for KEY002, comment=Change 3 for key KEY002, timestamp=19]
        KEY003=Payload[title=Some Title for KEY003, comment=Change 4 for key KEY003, timestamp=20]
        KEY004=Payload[title=Some Title for KEY004, comment=Change 3 for key KEY004, timestamp=13]
         */

        /* -- Benchmark -- */
        /* ... */

    }
    
    private static void printRange(MultiVersionMap<String, Payload> mvm, String from, boolean fromInc, String to, boolean toInc, long t) {
        Iterator<Map.Entry<String, Payload>> it = mvm.rangeSnapshot(from, fromInc, to, toInc, t);
        while (it.hasNext()) {
            var e = it.next();
            System.out.println(e.getKey() + " = " + e.getValue());
        }
    }

    private static void benchmarkVariant(String label, List<Map.Entry<String, Payload>> rowList, long[] probes, int runs, VersionListFactory<Payload> factory, FlushableKVStore kv, Serializer<Payload> serializer) {
        System.out.println("\nBenchmark: " + label);

        List<Double> insertMs = new ArrayList<>();
        List<Double> queryMs = new ArrayList<>();

        for (int r = 0; r < runs; r++) {
            kv.flushDB();

            MultiVersionMap<String, Payload> mvm = new BackedSimpleMVM<>(factory, kv, serializer);
            
            long t0 = nowTime();
            for (var e: rowList)
                mvm.append(e.getKey(), e.getValue());
            long t1 = nowTime();
            insertMs.add(milliSeconds(t1 - t0));

            long q0 = nowTime();
            for (long ts :  probes) {
                Iterator<Map.Entry<String, Payload>> iter = mvm.snapshot(ts);
                while (iter.hasNext()) {
                    iter.next();
                }
            }
            long q1 = nowTime();
            queryMs.add(milliSeconds(q1 - q0));
        }
        
        System.out.printf(Locale.ROOT, "Insert: Average = %.3f ms (runs = %d) %n", averageCalc(insertMs), runs);
        System.out.printf(Locale.ROOT, "Snapshots over %d probes: Average = %.3f ms (runs = %d) %n", probes.length, averageCalc(queryMs), runs);
    }
    
    public static void benchmarkVWeaver(String label, List<Map.Entry<String, Payload>> rowList, long[] probes, int runs, FlushableKVStore kv, Serializer<Payload> serializer) {
        System.out.println("\nBenchmark: " + label);
    
        List<Double> insertMs = new ArrayList<>();
        List<Double> queryMs = new ArrayList<>();
    
        for (int r = 0; r < runs; r++) {
            kv.flushDB();
    
            MultiVersionMap<String, Payload> mvm = new BackedVWeaverMVM<>(kv, serializer);
            
            long t0 = nowTime();
            for (var e: rowList)
                mvm.append(e.getKey(), e.getValue());
            long t1 = nowTime();
            insertMs.add(milliSeconds(t1 - t0));
    
            long q0 = nowTime();
            for (long ts :  probes) {
                Iterator<Map.Entry<String, Payload>> iter = mvm.snapshot(ts);
                while (iter.hasNext()) {
                    iter.next();
                }
            }
            long q1 = nowTime();
            queryMs.add(milliSeconds(q1 - q0));
        }
        
        System.out.printf(Locale.ROOT, "Insert: Average = %.3f ms (runs = %d) %n", averageCalc(insertMs), runs);
        System.out.printf(Locale.ROOT, "Snapshots over %d probes: Average = %.3f ms (runs = %d) %n", probes.length, averageCalc(queryMs), runs);
        
    }
}
