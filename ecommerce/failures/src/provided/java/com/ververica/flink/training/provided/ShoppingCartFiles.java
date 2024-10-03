package com.ververica.flink.training.provided;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public class ShoppingCartFiles {

    public static FileSource<String> makeCartFilesSource() throws URISyntaxException {
        URL srcPathAsURL = ShoppingCartFiles.class.getResource("/cart-files/file-001.txt");
        Path srcPath = new Path(srcPathAsURL.toURI());

        // Create a stream of ShoppingCartRecords from the directory we just filled with files.
        return FileSource.forRecordStreamFormat(new TextLineInputFormat("UTF-8"),
                        srcPath.getParent())
                .processStaticFileSet()
                .build();
    }

    public static Sink<String> makeResultFilesSink(Path resultsDir) {
        return FileSink.forRowFormat(resultsDir, new SimpleStringEncoder()).build();
    }

    public static FileSource<String> makeResultFilesSource(Path resultsDir) {
        return FileSource.forRecordStreamFormat(new TextLineInputFormat("UTF-8"),
                        resultsDir)
                .build();
    }
}
