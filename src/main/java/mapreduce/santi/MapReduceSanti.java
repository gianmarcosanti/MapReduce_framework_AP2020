package mapreduce.santi;

import java.nio.file.Paths;

public class MapReduceSanti {
    public static void main(String[] args) {
        CountingWords cw = new CountingWords(Paths.get("/Users/gianmarcosanti/Library/Mobile Documents/com~apple~CloudDocs/UNIPI/AdvancedProgramming/SecondAssignment/Exercise3/Books"));
        cw.compute();
        InvertedIndex index = new InvertedIndex(Paths.get("/Users/gianmarcosanti/Library/Mobile Documents/com~apple~CloudDocs/UNIPI/AdvancedProgramming/SecondAssignment/Exercise3/Books"));
        index.compute();
    }
}
