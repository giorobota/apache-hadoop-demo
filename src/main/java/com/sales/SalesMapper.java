package com.sales;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public final class SalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	private final Text category = new Text();
	public final void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
	{
		final String line = value.toString();	

		final String[] data = line.trim().split("\t");
		//data[0]= id					A
		//data[1]= prices.amountMax		B
		//data[2]= prices.amountMin		C
		//data[3]= prices.availability	D

		//data[4]= prices.currency		F
		//data[5]= prices.dateSeen		G

		//data[6]= prices.merchant		I

		//data[7]= asins				L
		//data[8]= brand				M
		//data[9]= categories			N
		//data[10]= dateAdded			O
		//data[11]= dateUpdated			P

		//data[12]= imageURLs			R

		//data[13]= manufacturer		T
		//data[14]= manufacturerNumber	U
		//data[15]= name				V
		//data[16]= primaryCategories	W

		//data[17]= weight				Z

		String slist[] = { "ASUS","APPLE","DELL","LENOVO","SAMSUNG","SONY","TOSHIBA" };
		List<String> list = Arrays.asList(slist);

		if (data.length == 18)
		{
			if(list.contains(String.valueOf(data[8]).toUpperCase())) {
				final String brand = data[8];
				final double amountMax = Double.parseDouble(data[1]);
				category.set(brand);
				context.write(category, new DoubleWritable(amountMax));
			}

//			if(list.stream().anyMatch(data[8]::equalsIgnoreCase)) {
////				System.out.println("data[8]: " + data[8] + " | data[1]: " + data[1]);
//				final String brand = data[8];
//				final double amountMax = Double.parseDouble(data[1]);
//				category.set(brand);
//				context.write(category, new DoubleWritable(amountMax));
////				(ASUS,202.12) 	output to reducer
//			}
		}
	}
}