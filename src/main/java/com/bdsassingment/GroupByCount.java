package com.bdsassingment;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

import static com.bdsassingment.Jobs.groupByAndCount;

public class GroupByCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = groupByAndCount(args[2].split(","), new Path(args[0]), new Path(args[1]), 4);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
