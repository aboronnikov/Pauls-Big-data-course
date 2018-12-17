package com.epam.spring;

import com.epam.program.Line;
import com.epam.program.TopThreeCalculator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.annotation.YarnComponent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Bean, that executes all the necessary actions.
 */
@YarnComponent
public class JobPojo {

    /**
     * Default logger.
     */
    private static final Log log = LogFactory.getLog(JobPojo.class);

    /**
     * Config, autowired from application.yml.
     */
    @Autowired
    private Configuration configuration;

    /**
     * Utility function that converts a String line into a line abstraction.
     *
     * @param line string line.
     * @return line abstraction.
     */
    private static Line getLine(String line) {
        return new Line(line);
    }

    /**
     * This is the action to be executed.
     * It first reads all of the lines from our file,
     * converts them to line abstraction,
     * and then executes the calculateTopThree function on them.
     *
     * @throws IOException exception that is propagated to the user in case of failure.
     */
    @OnContainerStart
    public void calculateTopThreeHotels() throws IOException {

        String fileString = "/user/test.csv";

        List<Line> lines = new ArrayList<>();
        Path path = new Path(fileString);

        try (FileSystem fs = FileSystem.get(configuration);
             BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {

            String line = br.readLine();
            while (line != null) {
                lines.add(getLine(line));
                line = br.readLine();
            }
        }

        List<Map.Entry<Line, Long>> results = TopThreeCalculator.calculateTopThree(lines);
        results.forEach(entry -> log.info(entry.getValue()));
    }
}
