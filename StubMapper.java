package com.cloudxlab.wordcount;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StubMapper extends Mapper<Object, Text, Text, LongWritable> {

	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer words = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z0-9 \t]", "").toLowerCase());
		String prevToken = null;
		if (words.hasMoreTokens()) {
			prevToken = words.nextToken();
		}

		String currentToken = null;

		while (words.hasMoreTokens()) {

			currentToken = words.nextToken();
			if (!currentToken.isEmpty() ) {
			if (!prevToken.isEmpty() ) {
				word.set(prevToken + " " + currentToken); // keyimiz önceki kelime ve sonraki kelime

				Text outKey = new Text(word);
				LongWritable outValue = new LongWritable(1);

				context.write(outKey, outValue);
				prevToken = currentToken; // þimdiki kelimeyi bir sonraki olarak ata
			}
			}
		}
	}
}