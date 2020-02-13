package mapreduce.santi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CountingWords extends MapReduce<String, String, Integer, Integer> {

    public CountingWords(Path path) {
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

    @Override
    protected int compare(String key1, String key2) {
        return key1.compareTo(key2);
    }

    /**
     * This method takes care of counting the occurrences of each word inside the line in which it is located. In the first
     * place it makes sure of eliminating all the special symbols that may corrupt the comparison of two words and also
     * making them all in lower cases in order to make the comparison easier. Once the input stream is "cleaned up" it proceeds
     * on counting how many times each word occurs in its line.
     * @param pairStream stream of pairs containing as key the name of the document and as value the list of that file's rows
     * @return stream of pair containing as key the word and as value the occurrences of that word in a certain line
     */
    @Override
    protected Stream<Pair<String, Integer>> map(Stream<Pair<String, List<String>>> pairStream) {

        return pairStream
                .flatMap(rows -> rows.value.stream())
                .flatMap(row ->
                        Arrays.asList(
                                row.trim().replaceAll("[.,:;!?\\-\"'()_\\[\\]—’‘“”]+", " ").split(" "))
                                .stream()
                                .filter(word -> word.length() > 3)
                                .map(word -> word.toLowerCase())
                                .collect(
                                    Collectors.toMap(
                                            w -> w, w -> 1, Integer::sum))
                                .entrySet()
                                .stream()
                                .map(entry -> new Pair<String, Integer>( entry.getKey(), entry.getValue())));


    }

    /**
     * This method takes care of summing up all the occurrencies for the same word in order to count the global occurrence
     * of that word
     * @param pairStream stream of pairs containing as key the word and as value the list of its occurrences
     * @return stream of pairs containing as key the word and as value the number of occurrences in all the documents
     */
    @Override
    protected Stream<Pair<String, Integer>> reduce(Stream<Pair<String, List<Integer>>> pairStream) {
        return pairStream.map(pair -> new Pair<>(pair.getKey(), pair.getValue().stream().reduce(0, Integer::sum)));
    }

    /**
     * Writes each element of the given string in the file specifies by the given path
     * @param stream stream to write
     * @param path path of the file to write in
     */
    @Override
    protected void write(Stream stream, Path path) {
        Writer w = new Writer();

        File f = Paths.get(path.toAbsolutePath().toString(), "counting_word.csv").toFile();
        try {
            w.write(f, stream);
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        }

    }

}

