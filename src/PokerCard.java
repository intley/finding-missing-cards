/*
 * @author: Rahul Rajendran
 * @course: CS 644 Introduction to Big Data
 * @description: Finding missing poker cards using a mapReduce solution
 */

// Configuration libraries required for Hadoop & MapReduce
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PokerCard extends Configured implements Tool {

	public int run(String[] args) throws Exception { 
		Job conf = Job.getInstance(getConf(), "Finding missing PokerCards");
		conf.setJarByClass(getClass());
		
		TextInputFormat.addInputPath(conf, new Path(args[0]));
		conf.setInputFormatClass(TextInputFormat.class);
		
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
