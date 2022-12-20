package com.sales;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public final class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
	public final void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException
	{
		double sum = 0.0;
		long count = 0;
		for (final DoubleWritable val : values)
		{
			sum += val.get();
			count++;
		}
		context.write(key, new DoubleWritable(sum));	//(men's clothing,1000.00)
		context.write(key, new DoubleWritable(count));	//(men's clothing,1000.00)
	}
}
