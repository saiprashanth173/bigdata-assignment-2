package com.bdsassingment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

import static com.bdsassingment.Defaults.DEFAULT_LIMIT;
import static com.bdsassingment.Defaults.DEFAULT_REDUCERS;
import static com.bdsassingment.Jobs.groupByAndCount;
import static com.bdsassingment.Jobs.topNRecords;

public class GroupByCountAndLimit {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        Path temporaryOutput = new Path("/tmpCountsGroupByCountAndLimit");
        int reducers = DEFAULT_REDUCERS;
        if (args.length == 5) {
            reducers = Integer.parseInt(args[4]);
        }

        Job jobGroupAndCount = groupByAndCount(args[2].split(","), new Path(args[0]), temporaryOutput, reducers);
        if (!jobGroupAndCount.waitForCompletion(true)) {
            System.exit(1);
        }

        int limit = DEFAULT_LIMIT;
        if (args.length >= 4) {
            limit = Integer.parseInt(args[3]);
        }

        Job jobLimit = topNRecords(limit, temporaryOutput, new Path(args[1]));
        int status = jobLimit.waitForCompletion(true) ? 0 : 1;
        temporaryOutput.getFileSystem(config).delete(temporaryOutput, true);
        System.exit(status);
    }
}
