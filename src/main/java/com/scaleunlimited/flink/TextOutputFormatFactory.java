package com.scaleunlimited.flink;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;

@SuppressWarnings("serial")
public class TextOutputFormatFactory<T> implements OutputFormatFactory<T> {

    private Path _basePath;
    private String _pattern;
    private WriteMode _mode;
    
    public TextOutputFormatFactory(Path basePath, String pattern, WriteMode mode) {
        _basePath = basePath;
        _pattern = pattern;
        _mode = mode;
    }
    
    @Override
    public OutputFormat<T> makeOutputFormat(String bucket) {
        String extension = _pattern.replaceAll("%s", bucket);
        TextOutputFormat<T> result = new TextOutputFormat<T>(new Path(_basePath, extension), StandardCharsets.UTF_8.name());
        result.setWriteMode(_mode);
        return result;
    }

}
