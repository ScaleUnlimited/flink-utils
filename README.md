# flink-utils

Utilities for use with Flink (streaming and batch)

## Building

Run `mvn clean package` to create the `flink-utils-1.0-SNAPSHOT.jar` in the `target` sub-directory.

## BucketingOutputFormat

A Flink batch API OutputFormat "wrapper" that supports writing records to files or directories based on data found in the record.

### Usage

A `BucketingOutputFormat` takes an `OutputFormatFactory` object that is used to create a Flink `OutputFormat` on demand, 
and a `KeySelector` to extract 
a `String` from an incoming record. The extracted string is then passed to the `OutputFormatFactory.makeOutputFormat()` method
as the `bucket` parameter, which the factory uses when constructing an appropriate `OutputFormat`.

A `TextOutputFormatFactory` example implementation is included, which creates a `TextOutputFormat` on demand. This class's
constructor takes a base `Path` parameter, a String pattern, and a `WriteMode`. The destination path for any new
`TextOutputFormat` is constructed by replacing all occurrences of `%s` in the pattern with the provided bucket string,
and then appending this to the base path.

For example...

``` java
    Path pathToOutputDir = "hdfs://working-data/results/";
    String filenamePattern = "dataset_%s.txt";
    
    DataSet<SomePOJO> records
        .partitionByHash(r -> r.getLanguageCode())
        .output(new BucketingOutputFormat<>(
            new TextOutputFormatFactory<SomePOJO>(pathToOutputDir, filenamePattern, WriteMode.OVERWRITE),
            r -> r.getLanguageCode()))
```

In the above code, `SomePOJO` has a `.getLanguageCode()` method that returns the two letter language code from the
record. So the `hdfs://working-data/results/` directory will wind up containing files with names like `dataset_en.txt`, 
`dataset_ja.txt`, etc.

Note the use of `.partitionByHash(<same key as used for bucket names>)`, to limit the number of unique buckets per task.

The `BucketingOutputFormat` runs in "file-only" mode by default, where each different bucket is a single file. You can
disable this by calling `BucketingOutputFormat.setForceFiles(false)`, in which case each bucket's output format is opened with
the number of tasks equal to sink's actual parallelism; for TextOutputFormatFactory, this will mean you get per-bucket
directories instead of files, if the sink's parallelism is > 1.
