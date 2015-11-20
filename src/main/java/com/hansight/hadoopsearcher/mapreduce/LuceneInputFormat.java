package com.hansight.hadoopsearcher.mapreduce;

import com.hansight.hadoopsearcher.util.HadoopCompat;
import com.hansight.hadoopsearcher.util.HadoopUtils;
import com.hansight.hadoopsearcher.util.HdfsUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Base class for input formats that read lucene indexes stored in HDFS directories.
 * Given a list of indexes and queries, runs each query over each index. Implements split
 * combining (combines multiple indexes into one split) based on
 * the total size of the index directory and the configured max combined split size.
 * <p>
 * Emits key, value records where key is the query that resulted in value
 * (key is actually the position in the list of queries, not the query string itself)
 * <p>
 * Subclasses must provide:
 * <ul>
 *  <li>a {@link LuceneRecordReader} which describes how to convert a String into a
 *      Query and how to convert a Document into a value of type T</li>
 * </ul>
 * Subclasses may provide:
 * <ul>
 *   <li> a {@link PathFilter} for identifying HDFS directories that contain Lucene indexes</li>
 * </ul>
 *
 * @param <T> - the type that your lucene Documents will be converted to
 * @author zhachao
 */
public abstract class LuceneInputFormat<T extends Writable> extends InputFormat<IntWritable, T>{

    public static final String QUERIES_KEY = LuceneInputFormat.class.getCanonicalName() + ".queries";

    public static final String INPUT_PATHS_KEY = LuceneInputFormat.class.getCanonicalName() + ".inputpaths";

    public static final String MAX_NUM_INDEXES_PER_SPLIT_KEY = LuceneInputFormat.class.getCanonicalName() + ".max_num_indexes_per_split";

    private static final long DEFAULT_MAX_NUM_INDEXES_PER_SPLIT = 200;

    public static final String MAX_COMBINED_INDEX_SIZE_PER_SPLIT_KEY = LuceneInputFormat.class.getCanonicalName() + ".max_combined_index_size_per_split";

    private static final long DEFAULT_MAX_COMBINED_INDEX_SIZE_PER_SPLIT = 10*1024*1024*1024L;

    private static final String[] EMPTY_NODE_ARRAY = new String[0];

    private Path[] inputPaths = null;
    private PathFilter indexDirPathFilter = null;
    private long maxCombinedIndexSizePerSplit;
    private long maxNumIndexesPerSplit;

    public PathFilter getIndexDirPathFilter(Configuration conf) throws IOException {
        return LuceneOutputFormat.newIndexDirFilter(conf);
    }

    public void loadConfig(Configuration conf) throws IOException {
        inputPaths = getInputPaths(conf);

        indexDirPathFilter = Preconditions.checkNotNull(getIndexDirPathFilter(conf),
                "You must provide a non-null PathFilter");

        maxCombinedIndexSizePerSplit = Preconditions.checkNotNull(
                getMaxCombinedIndexSizePerSplit(conf),
                MAX_COMBINED_INDEX_SIZE_PER_SPLIT_KEY + " cannot be null");

        maxNumIndexesPerSplit = Preconditions.checkNotNull(getMaxNumIndexesPerSplit(conf),
                MAX_NUM_INDEXES_PER_SPLIT_KEY + " cannot be null");
    }


    public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {

        // load settings from job conf
        loadConfig(HadoopCompat.getConfiguration(job));

        // find all the index dirs and create a split for each
        PriorityQueue<LuceneInputSplit> splits = findSplits(HadoopCompat.getConfiguration(job));

        // combine the splits based on maxCombineSplitSize
        List<InputSplit> combinedSplits = combineSplits(splits, maxCombinedIndexSizePerSplit,
                maxNumIndexesPerSplit);

        return combinedSplits;
    }

    public PriorityQueue<LuceneInputSplit> findSplits(Configuration conf) throws IOException {
        PriorityQueue<LuceneInputSplit> splits = new PriorityQueue<LuceneInputSplit>();
        List<Path> indexDirs = Lists.newLinkedList();

        // find all indexes nested under all the input paths
        // (which happen to be directories themselves)
        for (Path path : inputPaths) {
            HdfsUtils.collectPaths(path, path.getFileSystem(conf), indexDirPathFilter, indexDirs);
        }

        // compute the size of each index
        // and create a single split per index
        for (Path indexDir : indexDirs) {
            long size = HdfsUtils.getDirectorySize(indexDir, indexDir.getFileSystem(conf));
            splits.add(new LuceneInputSplit(Lists.newLinkedList(Arrays.asList(indexDir)), size));
        }
        return splits;
    }

    public List<InputSplit> combineSplits(PriorityQueue<LuceneInputSplit> splits,
                                          long maxCombinedIndexSizePerSplit,
                                          long maxNumIndexesPerSplit) {

        // now take the one-split-per-index splits and combine them into multi-index-per-split splits
        List<InputSplit> combinedSplits = Lists.newLinkedList();
        LuceneInputSplit currentSplit = splits.poll();
        while (currentSplit != null) {
            while (currentSplit.getLength() < maxCombinedIndexSizePerSplit) {
                LuceneInputSplit nextSplit = splits.peek();
                if (nextSplit == null) {
                    break;
                }
                if (currentSplit.getLength() + nextSplit.getLength() > maxCombinedIndexSizePerSplit) {
                    break;
                }
                if (currentSplit.getIndexDirs().size() >= maxNumIndexesPerSplit) {
                    break;
                }
                currentSplit.combine(nextSplit);
                splits.poll();
            }
            combinedSplits.add(currentSplit);
            currentSplit = splits.poll();
        }
        return combinedSplits;
    }

    public static void setQueries(List<String> queries, Configuration conf) throws IOException {
        Preconditions.checkNotNull(queries);
        Preconditions.checkArgument(!queries.isEmpty());
        HadoopUtils.writeStringListToConfAsBase64(QUERIES_KEY, queries, conf);
    }


    public static List<String> getQueries(Configuration conf) throws IOException {
        return Preconditions.checkNotNull(HadoopUtils.readStringListFromConfAsBase64(QUERIES_KEY, conf),
                "You must call LuceneIndexInputFormat.setQueries()");
    }

    public static boolean queriesSet(Configuration conf) {
        return conf.get(QUERIES_KEY) != null;
    }

    public static void setInputPaths(List<Path> paths, Configuration conf) throws IOException {
        Preconditions.checkNotNull(paths);
        Preconditions.checkArgument(!paths.isEmpty());
        String[] pathStrs = new String[paths.size()];
        int i = 0;
        for (Path p : paths) {
            FileSystem fs = p.getFileSystem(conf);
            pathStrs[i++] = fs.makeQualified(p).toString();
        }
        conf.setStrings(INPUT_PATHS_KEY, pathStrs);
    }

    public static Path[] getInputPaths(Configuration conf) {
        String[] pathStrs = Preconditions.checkNotNull(conf.getStrings(INPUT_PATHS_KEY),
                "You must call LuceneIndexInputFormat.setInputPaths()");
        Path[] paths = new Path[pathStrs.length];
        for (int i = 0; i < pathStrs.length; i++) {
            paths[i] = new Path(pathStrs[i]);
        }
        return paths;
    }

    public static void setMaxCombinedIndexSizePerSplitBytes(long size, Configuration conf) {
        conf.setLong(MAX_COMBINED_INDEX_SIZE_PER_SPLIT_KEY, size);
    }

    public static long getMaxCombinedIndexSizePerSplit(Configuration conf) {
        return conf.getLong(MAX_COMBINED_INDEX_SIZE_PER_SPLIT_KEY,
                DEFAULT_MAX_COMBINED_INDEX_SIZE_PER_SPLIT);
    }

    public static void setMaxNumIndexesPerSplit(long num, Configuration conf) {
        conf.setLong(MAX_NUM_INDEXES_PER_SPLIT_KEY, num);
    }

    public static long getMaxNumIndexesPerSplit(Configuration conf) {
        return conf.getLong(MAX_NUM_INDEXES_PER_SPLIT_KEY, DEFAULT_MAX_NUM_INDEXES_PER_SPLIT);
    }





    public static class LuceneInputSplit extends InputSplit
            implements Writable, Comparable<LuceneInputSplit> {
        private List<Path> indexDirs;
        private Long length;

        /**
         * Required for instantiation by reflection
         */
        public LuceneInputSplit() { }

        /**
         * Constructor for this class.
         * @param indexDirs a {@link java.util.List} of directories
         * containing existing Lucene indexes.
         * @param length the size of the split in bytes
         */
        public LuceneInputSplit(List<Path> indexDirs, long length) {
            this.indexDirs = indexDirs;
            this.length = length;
        }

        /**
         * Merge other into this split
         * Will have no effect on other
         * @param other the split to combine
         */
        public void combine(LuceneInputSplit other) {
            indexDirs.addAll(other.getIndexDirs());
            length += other.getLength();
        }

        /**
         * Get the size of this split in bytes
         * @return the size of this split in bytes
         */
        @Override
        public long getLength() {
            return length;
        }

        /**
         * Because an index consists of multiple (multi-block) files there's not much to be gained from
         * finding nodes where there is locality
         * @return an empty String[]
         */
        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return EMPTY_NODE_ARRAY;
        }

        /**
         * @return the list of indexes in this split (which are directories)
         */
        public List<Path> getIndexDirs() {
            return indexDirs;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(length);
            out.writeInt(indexDirs.size());
            for(Path p : indexDirs) {
                Text.writeString(out, p.toString());
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.length = in.readLong();
            int numDirs = in.readInt();
            this.indexDirs = Lists.newLinkedList();
            for (int i = 0; i < numDirs; i++) {
                String path = Text.readString(in);
                this.indexDirs.add(new Path(path));
            }
        }

        /**
         * sorts by length (size in bytes)
         */
        @Override
        public int compareTo(LuceneInputSplit other) {
            return length.compareTo(other.getLength());
        }

        /**
         * Prints the directories and the combined length/size of the split in bytes.
         */
        @Override
        public String toString() {
            return "LuceneIndexInputSplit<indexDirs:" + indexDirs.toString() + " length:" + length + ">";
        }
    }


}
