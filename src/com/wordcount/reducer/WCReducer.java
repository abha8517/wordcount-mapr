package com.wordcount.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		try {
			Long sum = 0L;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
