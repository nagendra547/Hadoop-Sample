package com.nagendra547.hadoopworld;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author nagendra
 *
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final String EMPTY_SPACE  = " ";
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, EMPTY_SPACE);

		while (st.hasMoreTokens()) {
			word.set(st.nextToken());
			context.write(word, one);
		}

	}
}
