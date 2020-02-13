package mapreduce.santi;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author Gianmarco Santi
 */
public abstract class MapReduce<K1, K2, V1, V2> {

    private final Path path;

    public MapReduce(Path path) {
        this.path = path;
    }

    protected abstract Stream<Pair<K1, List<K1>>> read(Path path);

    protected abstract Stream<Pair<K2, V1>> map(Stream<Pair<K1, List<K1>>> pairStream);

    protected abstract Stream<Pair<K2, V2>> reduce(Stream<Pair<K2, List<V1>>> pairStream);

    protected abstract void write(Stream <Pair<K2, V2>> pairStream, Path path);

    protected abstract int compare(K2 key1, K2 key2);

    public void compute() {
        write(
                reduce(
                        combine(
                                map(
                                        read(this.path)
                                )
                        )
                ), this.path );
    }

    protected Stream<Pair<K2, List<V1>>> combine ( Stream<Pair<K2, V1>> pairStream){

        Map<K2, List<V1>> combinedMap = new TreeMap<>(this::compare);

        pairStream.forEach(pair -> {
            K2 key = pair.getKey();
            V1 value = pair.getValue();
            /*
                if the key has never been processed, associate an empty list to that key.
                Add a value to the associated list, otherwise.
             */
            combinedMap.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        });

        // returns a stream of key/{list of values} pairs.
        return combinedMap.entrySet().stream().map(entry -> new Pair<>(entry.getKey(), entry.getValue()));
    }
}
