package com.scaleunlimited.flink;

import java.io.Serializable;

import org.apache.flink.api.common.io.OutputFormat;

public interface OutputFormatFactory<T> extends Serializable {

    public OutputFormat<T> makeOutputFormat(String bucket);
}
