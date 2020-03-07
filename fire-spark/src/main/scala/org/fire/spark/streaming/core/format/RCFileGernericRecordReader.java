package org.fire.spark.streaming.core.format;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFile.KeyBuffer;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

public class RCFileGernericRecordReader implements RecordReader<LongWritable, Text> {
    private final Reader in;
    private final long start;
    private final long end;
    private boolean more = true;
    protected Configuration conf;
    private final FileSplit split;
    private final boolean useCache;

    public static final String RCFILE_TRANSFOR_TEXT_SEPERATOR = "rcfile.transform.text.seperator";
    private byte[] seperator;
    private BytesRefArrayWritable brawValue;

    private static RCFileSyncCache syncCache = new RCFileSyncCache();

    private static final class RCFileSyncEntry {
        long end;
        long endSync;
    }

    private static final class RCFileSyncCache {
        private final Map<String, RCFileSyncEntry> cache;
        public RCFileSyncCache() {
            cache = Collections.synchronizedMap(new WeakHashMap<String, RCFileSyncEntry>());
        }

        public void put(FileSplit split, long endSync) {
            Path path = split.getPath();
            long end = split.getStart() + split.getLength();
            String key = path.toString()+"+"+String.format("%d",end);

            RCFileSyncEntry entry = new RCFileSyncEntry();
            entry.end = end;
            entry.endSync = endSync;
            if(entry.endSync >= entry.end) {
                cache.put(key, entry);
            }
        }

        public long get(FileSplit split) {
            Path path = split.getPath();
            long start = split.getStart();
            String key = path.toString()+"+"+String.format("%d",start);
            RCFileSyncEntry entry = cache.get(key);
            if(entry != null) {
                return entry.endSync;
            }
            return -1;
        }
    }

    public RCFileGernericRecordReader(Configuration conf, FileSplit split)
            throws IOException {
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(conf);
        this.in = new RCFile.Reader(fs, path, conf);
        this.end = split.getStart() + split.getLength();
        this.conf = conf;
        this.split = split;
        useCache = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEUSERCFILESYNCCACHE);
        seperator = conf.get(RCFILE_TRANSFOR_TEXT_SEPERATOR,
                "\t").getBytes(Charsets.UTF_8);
        brawValue = new BytesRefArrayWritable();
        if (split.getStart() > in.getPosition()) {
            long oldSync = useCache ? syncCache.get(split) : -1;
            if(oldSync == -1) {
                in.sync(split.getStart()); // sync to start
            } else {
                in.seek(oldSync);
            }
        }
        this.start = in.getPosition();
        more = start < end;
    }

    /* （非 Javadoc）
     * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object, java.lang.Object)
     */
    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
        more = next(key);
        if (more) {
            value.clear();
            in.getCurrentRow(brawValue);
            if(brawValue.size() > 0) {
                BytesRefWritable v = brawValue.get(0);
                //必须写明getStart, getLength，因为RcFile是列簇传进来，getData获取的是这一列多行的数据
                value.append(v.getData(),v.getStart(),v.getLength());
                for(int i = 1; i < brawValue.size(); i++){
                    v = brawValue.get(i);
                    value.append(seperator, 0, seperator.length);
                    value.append(v.getData(),v.getStart(),v.getLength());
                }
            }
        }
        return more;
    }

    /* （非 Javadoc）
     * @see org.apache.hadoop.mapred.RecordReader#createKey()
     */
    @Override
    public LongWritable createKey() {
        return (LongWritable) ReflectionUtils.newInstance(getKeyClass(), conf);
    }

    /* （非 Javadoc）
     * @see org.apache.hadoop.mapred.RecordReader#createValue()
     */
    @Override
    public Text createValue() {
        return (Text) ReflectionUtils.newInstance(getValueClass(), conf);
    }

    /* （非 Javadoc）
     * @see org.apache.hadoop.mapred.RecordReader#getPos()
     */
    @Override
    public long getPos() throws IOException {
        return in.getPosition();
    }

    /* （非 Javadoc）
     * @see org.apache.hadoop.mapred.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        in.close();
    }

    /* （非 Javadoc）
     * @see org.apache.hadoop.mapred.RecordReader#getProgress()
     */
    @Override
    public float getProgress() throws IOException {
        if (end == start) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (in.getPosition() - start) / (float) (end - start));
        }
    }

    public Class<?> getKeyClass() {
        return LongWritable.class;
    }

    public Class<?> getValueClass() {
        return Text.class;
    }

    public boolean nextBlock() throws IOException {
        return in.nextBlock();
    }

    protected boolean next(LongWritable key) throws IOException {
        if (!more) {
            return false;
        }
        more = in.next(key);
        long lastSeenSyncPos = in.lastSeenSyncPos();
        if (lastSeenSyncPos >= end) {
            if(useCache) {
                syncCache.put(split, lastSeenSyncPos);
            }
            more = false;
            return more;
        }
        return more;
    }

    public KeyBuffer getKeyBuffer() {
        return in.getCurrentKeyBufferObj();
    }

    protected void seek(long pos) throws IOException {
        in.seek(pos);
    }

    public void sync(long pos) throws IOException {
        in.sync(pos);
    }

    public void resetBuffer() {
        in.resetBuffer();
    }

    public long getStart() {
        return start;
    }
}
