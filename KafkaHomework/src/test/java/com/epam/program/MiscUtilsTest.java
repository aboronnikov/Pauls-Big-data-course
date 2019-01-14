package com.epam.program;

import com.epam.mocks.TestData;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Test suite for MiscUtils.
 */
public class MiscUtilsTest {

    /**
     * Tests the method with the same name.
     */
    @Test
    public void makeKey() {
        String expected = "PavelOrekhov;Test3, Russia";
        String actual = MiscUtils.makeKey(TestData.TEST3);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Tests the method with the same name.
     */
    @Test
    public void extractHashtags() {
        List<String> expected = Collections.singletonList("PavelOrekhov");
        List<String> actual = MiscUtils.extractHashtags(TestData.TEST3);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Tests the method with the same name.
     */
    @Test
    public void extractLocation() {
        String expected = "Test3, Russia";
        String actual = MiscUtils.extractLocation(TestData.TEST3);
        Assert.assertEquals(expected, actual);
    }
}