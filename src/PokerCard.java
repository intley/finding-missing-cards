/*
 * @author: Rahul Rajendran
 * @course: CS 644 Introduction to Big Data
 * @description: Finding missing poker cards using a mapReduce solution
 * 
 * Hosted on GitHub at : https://github.com/intley/finding-missing-cards
 * 
 * References: 
 * https://hadoop.apache.org/docs/r2.7.4/api/org/apache/hadoop/mapreduce/Mapper.html
 * https://hadoop.apache.org/docs/r2.7.0/api/org/apache/hadoop/mapreduce/Reducer.html
 * https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/io/LongWritable.html
 * https://www.youtube.com/watch?v=he8vt835cf8&t=235s
 * 
 */

// Java libraries
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Configuration libraries required for Hadoop & MapReduce
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PokerCard extends Configured implements Tool {

	// Configuration required to run Mapreduce programs
	public int run(String[] args) throws Exception { 
		Job conf = Job.getInstance(getConf(), "Finding missing PokerCards");
		conf.setJarByClass(PokerCard.class);
		
		TextInputFormat.addInputPath(conf, new Path(args[0]));
		conf.setInputFormatClass(TextInputFormat.class);
		
		conf.setMapperClass(CardMapper.class);
	    conf.setReducerClass(CardReducer.class);
		
		TextOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputFormatClass(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		if (conf.waitForCompletion(true)) {
			return 0;
		}
		else {
			return 1;
		}

	}
	
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new PokerCard(), args);
		System.exit(status);
	}

}

// Mapper class maps the input file into Suit and Rank for the Reducer class
class CardMapper extends Mapper <LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		String [] field = line.split(":");
		String suit = field[0], rank = field[1];
		context.write(new Text(suit), new IntWritable(Integer.parseInt(rank)));
	}
}

// Reducer class takes the output of the mapper and performs the check for the missing cards 
// and writes the output
class CardReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key, Iterable<IntWritable> cards, Context context) throws IOException, InterruptedException {
		List<Integer> list = new ArrayList<>();
		int rankSum = 0;
		for (IntWritable card : cards) {
			int rank = card.get();
			list.add(rank);
			rankSum += rank;			
		}
		
		// Rank Sum must be equal to 91 since sum of ranks 1 .. 13 = 91
		if (rankSum < 91) {
			for (int i = 1; i <= 13; i ++) {
				if (!list.contains(i)) {
					context.write(key, new IntWritable(i));
				}
			}
		}
	}
}
