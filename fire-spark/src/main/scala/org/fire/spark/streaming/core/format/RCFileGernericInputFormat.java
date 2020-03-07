package org.fire.spark.streaming.core.format;


import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class RCFileGernericInputFormat extends FileInputFormat<LongWritable, Text> implements InputFormatChecker{

    public RCFileGernericInputFormat() {
        setMinSplitSize(SequenceFile.SYNC_INTERVAL);
    }

    /* （非 Javadoc）
     * @see org.apache.hadoop.mapred.FileInputFormat#getRecordReader(org.apache.hadoop.mapred.InputSplit, org.apache.hadoop.mapred.JobConf, org.apache.hadoop.mapred.Reporter)
     */
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(
            InputSplit split, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(split.toString());
        return new RCFileGernericRecordReader(job, (FileSplit) split);
    }

    @Override
    public boolean validateInput(FileSystem fs, HiveConf conf,
                                 ArrayList<FileStatus> files) throws IOException {
        if (files.size() <= 0) {
            return false;
        }
        for (int fileId = 0; fileId < files.size(); fileId++) {
            RCFile.Reader reader = null;
            try {
                reader = new RCFile.Reader(fs, files.get(fileId).getPath(), conf);
                reader.close();
                reader = null;
            } catch (IOException e) {
                return false;
            } finally {
                if (null != reader) {
                    reader.close();
                }
            }
        }
        return true;
    }
}
