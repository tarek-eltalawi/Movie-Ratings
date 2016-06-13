import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatings{

  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, DoubleWritable>{

    private IntWritable movie = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	StringTokenizer itr = new StringTokenizer(value.toString(), "::");
		itr.nextToken();	// skip the user id
		int movieId = Integer.parseInt(itr.nextToken());
		double rating_number = Double.parseDouble(itr.nextToken());
		DoubleWritable rating = new DoubleWritable(rating_number);
		movie.set(movieId);
		context.write(movie, rating);
    }
  }

  public static class IntSumReducer extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {
    
	  private DoubleWritable result = new DoubleWritable();

	  public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
	      double sum = 0;
	      int count = 0;
	      for (DoubleWritable val : values) {
	        sum += val.get();
	        count ++;
	      }
	      
	      double average = sum / count;
	      result.set(average);
//	      MultipleOutputs<IntWritable, DoubleWritable> multipleOutputs = new MultipleOutputs<IntWritable, DoubleWritable>(context);
	      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "MovieRatings");
	job.setJarByClass(MovieRatings.class);
	job.setMapperClass(TokenizerMapper.class);
	job.setCombinerClass(IntSumReducer.class);
	job.setReducerClass(IntSumReducer.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(DoubleWritable.class);
	
//	LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
//	MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, IntWritable.class, DoubleWritable.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}