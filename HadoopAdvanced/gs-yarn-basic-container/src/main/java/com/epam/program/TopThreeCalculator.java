package com.epam.program;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A utility class. Calculates 3 most popular hotels.
 */
public class TopThreeCalculator {

    /**
     * Private constructor to prevent the creation of objects from this class.
     */
    private TopThreeCalculator() {

    }

    /**
     * A function that calculates 3 most popular hotels amongst couples.
     *
     * @param lines lines read from a file
     * @return result as a list.
     */
    public static List<Map.Entry<Line, Long>> calculateTopThree(Stream<String> lines) {
        final int LINES_TO_SKIP = 1;
        final int NUMBER_OF_RESULTS = 3;
        return lines.skip(LINES_TO_SKIP)
                .map(Line::new)
                .filter(Line::isCouple)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(NUMBER_OF_RESULTS)
                .collect(Collectors.toList());
    }
}
