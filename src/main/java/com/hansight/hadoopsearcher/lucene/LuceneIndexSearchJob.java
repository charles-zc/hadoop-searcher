package com.hansight.hadoopsearcher.lucene;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by zhachao on 15-9-25.
 */
public class LuceneIndexSearchJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new LuceneIndexSearch(), args);
        System.exit(res);
    }
}
