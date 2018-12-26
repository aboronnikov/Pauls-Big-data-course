package com.epam.spring;

import com.epam.program.Line;
import com.epam.program.TopThreeCalculator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.annotation.YarnComponent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

    @Value("${my.container.filePath}")
    private String filePath;

    /**
     * This is the action to be executed.
     * It first reads all of the lines from our file,
     * converts them to line abstraction,
     * and then executes the calculateTopThree function on them.
     */
    @OnContainerStart
    public void calculateTopThreeHotels() {
        Path path = new Path(filePath);
        try (FileSystem fs = FileSystem.get(configuration);
             BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)))) {

            List<Map.Entry<Line, Long>> results = TopThreeCalculator.calculateTopThree(bufferedReader.lines());
            results.forEach(e -> log.info("Answer: " + e.getValue()));
        } catch (IOException e) {
            log.error("Does your file exist?", e);
        }
    }
}
