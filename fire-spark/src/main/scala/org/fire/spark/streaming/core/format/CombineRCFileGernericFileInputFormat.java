package org.fire.spark.streaming.core.format;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

public class CombineRCFileGernericFileInputFormat extends
        CombineFileInputFormat<LongWritable, Text> {
    /* （非 Javadoc）
     * @see org.apache.hadoop.mapred.lib.CombineFileInputFormat#getRecordReader(org.apache.hadoop.mapred.InputSplit, org.apache.hadoop.mapred.JobConf, org.apache.hadoop.mapred.Reporter)
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
                                                            JobConf conf, Reporter reporter) throws IOException {
        return new CombineFileRecordReader(conf, (CombineFileSplit)split,
                reporter, RCFileRecordReaderWrapper.class);
    }

    private static class RCFileRecordReaderWrapper extends
            CombineFileRecordReaderWrapper<LongWritable, Text> {
        public RCFileRecordReaderWrapper(CombineFileSplit split, Configuration conf,
                                         Reporter reporter, Integer idx) throws IOException, InterruptedException {
            super(new RCFileGernericInputFormat(), split, conf, reporter, idx);
        }
    }
}
