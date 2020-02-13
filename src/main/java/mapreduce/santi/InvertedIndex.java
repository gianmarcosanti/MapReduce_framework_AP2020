package mapreduce.santi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InvertedIndex extends MapReduce<String, String, Pair<String,Integer>, String> {

    public InvertedIndex(Path path) {
        super(path);
    }

    /**
     * The function reads the file related to the given path by instantiating a new Reader object
     * @param path path of the file to be read
     * @return a stream of pairs containing as key the name of the document just read and as value the list of that file's rows
     */
    @Override
    protected Stream<Pair<String, List<String>>> read(Path path) {
        Reader reader = new Reader(path);
        try {
            return  reader.read();
        } catch (IOException e) {
            return Stream.empty();
        }
    }

    /**
     * This method takes care of mapping, for each file, each word with the line in which it is contained. In the first
     * place it associate at each line a unique number, then it makes sure of eliminating all the special symbols that
     * may corrupt the comparison of two words and also making them all in lower cases in order to make the comparisons easier.
     * Once the input stream is "cleaned up" it proceeds to associate each word to the file and the number of line in which
     * it is contained
     *
     * @param pairStream stream of pairs containing as key the name of the document and as value the list of that file's rows
     * @return a stream of pairs containing as key* the word and as value a pair with as key the name of the document and as value
     * the number of line in which the key* is contained
     */
    @Override
    protected Stream<Pair<String, Pair<String,Integer>>> map(Stream<Pair<String, List<String>>> pairStream) {
        AtomicInteger counter = new AtomicInteger(0);

        return pairStream
                .flatMap(docs -> docs.getValue().stream()
                                                .map( row ->  new Pair<>(row, new Pair<>(docs.getKey(),counter.getAndIncrement()))))
                .flatMap(rows -> Arrays.asList(rows.getKey().trim().replaceAll("[.,:;!?\\-\"'()_\\[\\]—’‘“”]+", " ").split(" "))
                        .stream()
                        .filter(word -> word.length() > 3)
                        .map(word -> new Pair<>(
                                word.toLowerCase(), new Pair<>(rows.getValue().getKey(), rows.getValue().getValue()))));

    }
    @Override
    protected int compare(String key1, String key2) {
        return key1.compareTo(key2);
    }

    /**
     * Reduce takes care of tranforming the input stream into a stream of pair in which the key is a word and the value is
     * one of the key's occurrences among all the documents formatted in the form: <filename, row number>
     * @param pairStream stream containing pairs of the type < word, list of pairs of type < filename, row number>>
     * @return stream of pairs containing a word and one of its occurrencies (<filename, row number>) formatted as string
     */
    @Override
    protected Stream<Pair<String, String>> reduce(Stream<Pair<String, List<Pair<String,Integer>>>> pairStream) {
        return pairStream.flatMap( pair -> pair.getValue()
                .stream()
                .map( line -> new Pair<>(pair.getKey(), formatPair(line)))
        );
    }

    private String formatPair (Pair<String, Integer> pair){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(pair.getKey());
        stringBuilder.append(", ");
        stringBuilder.append(pair.getValue());
        return  stringBuilder.toString();
    }

    /**
     * Writes each element of the given string in the file specifies by the given path
     * @param stream stream to write
     * @param path path of the file to write in
     */
    @Override
    protected void write(Stream stream, Path path) {
        Writer w = new Writer();

        File f = Paths.get(path.toAbsolutePath().toString(), "inverted_index.csv").toFile();
        try {
            w.write(f, stream);
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        }

    }

}

