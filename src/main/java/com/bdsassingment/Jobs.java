package com.bdsassingment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Jobs {
    static Job groupByAndCount(String[] columns, Path input, Path output, int reducers) throws IOException {
        Configuration config = new Configuration();
        config.setStrings("columns", columns);

        Job job = Job.getInstance(config, "Visitor-Frequency-Counter");
        job.setJobName("visitor-frequency-counter");
        job.setJarByClass(GroupByCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(VisitorMapper.class);
        job.setReducerClass(VisitorReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(reducers);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        output.getFileSystem(config).delete(output, true);
        return job;
    }

    static Job topNRecords(int limit, Path input, Path output) throws IOException {
        Configuration config = new Configuration();

        config.setInt("top", limit);

        Job job = Job.getInstance(config, "topN");
        job.setJarByClass(TopNDriver.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TopNMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TopNReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        output.getFileSystem(config).delete(output, true);
        return job;
    }

}
