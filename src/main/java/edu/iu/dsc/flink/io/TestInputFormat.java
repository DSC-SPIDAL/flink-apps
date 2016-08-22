package edu.iu.dsc.flink.io;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;

public class TestInputFormat extends FileInputFormat {

    @Override
    public InputSplitAssigner getInputSplitAssigner(
        InputSplit[] inputSplits) {
        return null;
    }

    @Override
    public void open(InputSplit split) throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public Object nextRecord(Object reuse) throws IOException {
        return null;
    }
}
