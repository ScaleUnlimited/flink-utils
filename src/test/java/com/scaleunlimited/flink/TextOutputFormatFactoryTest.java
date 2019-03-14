package com.scaleunlimited.flink;

import static org.junit.Assert.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.junit.Test;

public class TextOutputFormatFactoryTest {

    private static final String BASE_DIR = "target/test/TextOutputFormatFactoryTest/";
    
    @Test
    public void testTextOutputFormat() throws Exception {
        File baseDir = new File(BASE_DIR);
        File testDir = new File(baseDir, "testTextOutputFormat");
        FileUtils.deleteDirectory(testDir);
        testDir.mkdirs();

        System.setProperty("log.file", testDir.getAbsolutePath());
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(2);

        SomePOJO[] records = new SomePOJO[] {
                new SomePOJO("us", "hello"),
                new SomePOJO("us", "hi"),
                new SomePOJO("es", "hola"),
                new SomePOJO("fr", "bonjour"),
        };
        
        File outputDir = new File(testDir, "output");
        Path basePath = new Path(outputDir.getAbsolutePath());
        String filenamePattern = "dataset_%s.txt";

        OutputFormat<SomePOJO> output = new BucketingOutputFormat<>(
                new TextOutputFormatFactory<>(basePath, filenamePattern, WriteMode.OVERWRITE),
                r -> r.getCountryId());
        
        env.fromElements(records)
            .output(output);
        
        env.execute();

        // We should wind up with three files in the output directory.
        File unitedstates = new File(outputDir, "dataset_us.txt");
        List<String> lines = FileUtils.readLines(unitedstates, StandardCharsets.UTF_8);
        assertEquals(2, lines.size());
        assertTrue(lines.contains("hello"));
        assertTrue(lines.contains("hi"));
        
        File france = new File(outputDir, "dataset_fr.txt");
        lines = FileUtils.readLines(france, StandardCharsets.UTF_8);
        assertEquals(1, lines.size());
        assertTrue(lines.contains("bonjour"));
        
        File spain = new File(outputDir, "dataset_es.txt");
        lines = FileUtils.readLines(spain, StandardCharsets.UTF_8);
        assertEquals(1, lines.size());
        assertTrue(lines.contains("hola"));
    }
    
    private static class SomePOJO {
        private String _countryId;
        private String _text;
        
        public SomePOJO() {}

        public SomePOJO(String countryId, String text) {
            _countryId = countryId;
            _text = text;
        }

        public String getCountryId() {
            return _countryId;
        }

        public void setCountryId(String countryId) {
            _countryId = countryId;
        }

        public String getText() {
            return _text;
        }

        public void setText(String text) {
            _text = text;
        }
        
        @Override
        public String toString() {
            // Return just the text, so our output record doesn't
            // contain the country code (redundant, since we're bucketing)
            return _text;
        }
    }

}
