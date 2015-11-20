package com.hansight.hadoopsearcher.mapreduce;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.IntWritable;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by zhachao on 15-9-15.
 */
public abstract class LuceneCountHitsRecordReader extends LuceneRecordReader<IntWritable> {

    protected Iterator<IntWritable> search(IndexSearcher searcher, Query query) throws IOException {
        TotalHitCountCollector collector = new TotalHitCountCollector();
        searcher.search(query, collector);
        return ImmutableList.of(new IntWritable(collector.getTotalHits())).iterator();
    }


}
