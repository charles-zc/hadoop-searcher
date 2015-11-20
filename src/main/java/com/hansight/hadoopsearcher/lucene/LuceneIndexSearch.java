package com.hansight.hadoopsearcher.lucene;

import com.google.common.collect.Lists;
import com.hansight.hadoopsearcher.mapreduce.LuceneCollectAllRecordReader;
import com.hansight.hadoopsearcher.mapreduce.LuceneInputFormat;
import com.hansight.hadoopsearcher.mapreduce.LuceneOutputFormat;
import com.hansight.hadoopsearcher.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by zhachao on 15-9-24.
 */
public class LuceneIndexSearch extends Configured implements Tool {

    private static final List<String> QUERIES = Lists.newArrayList("proto:tcp");

    public final int run(String[] args) throws Exception {
        File f = new File("/home/zhachao/workspace/lucene-hadoop/search_results");
        if(f.exists()) f.delete();

        List<Path> indexes = Lists.newLinkedList();
        Path luceneIndexFile = new Path("/home/zhachao/index-0");
        indexes.add(luceneIndexFile);

        return doSearch(indexes, new Path(new File(tempDir.getRoot(), "search_results").getAbsolutePath()),
                "Failed searching indexes");
    }

    private int doSearch(List<Path> inputPaths, Path outputPath, String failureMessage) throws Exception{
        Job job = new Job(super.getConf());
        job.setJarByClass(LuceneIndexSearch.class);


        job.setInputFormatClass(IndexInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(SearchMapper.class);
        job.setReducerClass(SearchReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        IndexInputFormat.setInputPaths(inputPaths, HadoopCompat.getConfiguration(job));

        IndexInputFormat.setQueries(QUERIES, job.getConfiguration());

        TextOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        System.out.println(result);

        new File(new File(outputPath.toString()), "lucene-search-results");

        return result? 0 : -1;

    }

    private static TemporaryFolder tempDir = new TemporaryFolder();

    private static class IndexInputFormat extends LuceneInputFormat<Text> {

        public PathFilter getIndexDirPathFilter(Configuration conf) throws IOException {
            return LuceneOutputFormat.newIndexDirFilter(conf);
        }

        public RecordReader<IntWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {

            return new LuceneCollectAllRecordReader<Text>() {

                private QueryParser parser = new QueryParser(Version.LUCENE_4_10_4,
                        "text", new WhitespaceAnalyzer(Version.LUCENE_4_10_4));

                private Text text = new Text();

                public Query deserializeQuery(String serializedQuery) throws IOException {
                    try {
                        return parser.parse(serializedQuery);
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                }

                protected Text docToValue(Document doc) {
                    text.set(doc.get("text"));
                    return text;
                }
            };
        }
    }

    private static class SearchMapper extends Mapper<IntWritable, Text, IntWritable, Text> {
        protected void map(IntWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    private static class SearchReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

}
