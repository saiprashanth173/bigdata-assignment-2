package com.bdsassingment;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

import static com.bdsassingment.Defaults.DEFAULT_LIMIT;
import static com.bdsassingment.Jobs.topNRecords;
import static java.lang.Integer.parseInt;

public class TopNDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        int limit = DEFAULT_LIMIT;
        if (args.length > 2) {
            limit = parseInt(args[2]);
        }
        Job job = topNRecords(limit, new Path(args[0]), new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
