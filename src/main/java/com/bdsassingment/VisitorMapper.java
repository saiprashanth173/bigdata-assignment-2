package com.bdsassingment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class VisitorMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();
        List<Integer> groupBy = Arrays.stream(config.getStrings("columns")).map(Integer::parseInt).collect(Collectors.toList());
        String row = value.toString().toLowerCase();
        if (!row.contains(",") || (row.contains("namelast") && row.contains("namefirst")))
            return;
        String columns[] = row.split(",");

        StringBuilder emit = new StringBuilder();
        groupBy.forEach((x) -> {
            emit.append(columns[x]);
            emit.append(" ");
        });

        Text emitKey = new Text(emit.toString().trim());
        context.write(emitKey, one);
    }
}
