package com.epam.program;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test suite for the TopThreeCalculator class.
 */
public class TopThreeCalculatorTest {

    /**
     * Test for calculateTopThree function.
     *
     * The answer should be 4,3,1.
     * There are 4 continents of the first time,
     * 3 of the second type,
     * and 1 of the third type.
     */
    @Test
    public void calculateTopThree() {
        List<Line> lines = Arrays.asList(
                new Line("0,1,2,3,4,5,6,7,8,9,10,11,12,13,ADULT_COUNT,15,16,17,18,CONTINENT,20,21,22"),
                new Line("0,1,2,3,4,5,6,7,8,9,10,11,12,13,2,15,16,17,18,continent1,20,21,22"),
                new Line("0,1,2,3,4,5,6,7,8,9,10,11,12,13,3,15,16,17,18,continent1,20,21,22"),
                new Line("0,1,2,3,4,5,6,7,8,9,10,11,12,13,2,15,16,17,18,continent1,20,21,22"),
                new Line("0,1,2,3,4,5,6,7,8,9,10,11,12,13,2,15,16,17,18,continent1,20,21,22"),
                new Line("0,1,2,3,4,5,6,7,8,9,10,11,12,13,2,15,16,17,18,continent1,20,21,22"),
                new Line("0,1,2,3,4,5,6,7,8,9,10,11,12,13,2,15,16,17,18,continent2,20,21,22"),
                new Line("0,1,2,3,4,5,6,7,8,9,10,11,12,13,2,15,16,17,18,continent2,20,21,22"),
                new Line("0,1,2,3,4,5,6,7,8,9,10,11,12,13,2,15,16,17,18,continent2,20,21,22"),
                new Line("0,1,2,3,4,5,6,7,8,9,10,11,12,13,2,15,16,17,18,continent3,20,21,22")
        );

        List<Map.Entry<Line, Long>> results = TopThreeCalculator.calculateTopThree(lines);

        List<Long> expected = Arrays.asList(4L, 3L, 1L);
        List<Long> actual = results.stream().map(Map.Entry::getValue).collect(Collectors.toList());

        Assert.assertEquals(expected, actual);
    }
}