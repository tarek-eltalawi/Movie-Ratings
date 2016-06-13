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

public class Binning {

	public static class Mapper1 extends
			Mapper<Object, Text, IntWritable, DoubleWritable> {

		private IntWritable movie = new IntWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "::");
			itr.nextToken(); // skip the user id
			int movieId = Integer.parseInt(itr.nextToken());
			double rating_number = Double.parseDouble(itr.nextToken());
			DoubleWritable rating = new DoubleWritable(rating_number);
			movie.set(movieId);
			context.write(movie, rating);
		}
	}

	public static class Mapper2 extends
			Mapper<Object, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private IntWritable binNum = new IntWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString());
			st.nextToken(); // skip movieId.
			double movieRating = Double.parseDouble(st.nextToken());
			binNum.set(Math.min((int) (movieRating * 2) + 1, 10));
			context.write(binNum, one);
		}
	}

	public static class Reducer1 extends
			Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

		private DoubleWritable result = new DoubleWritable();

		public void reduce(IntWritable key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}

			double average = sum / count;
			result.set(average);
			context.write(key, result);
		}
	}

	public static class Reducer2 extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		String intermediateOutputPath = "intermediate";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Binning");
		job.setJarByClass(Binning.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(intermediateOutputPath));
		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		conf2.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
		Job job2 = Job.getInstance(conf2, "Movie Ratings");
		job2.setJarByClass(Binning.class);
		job2.setMapperClass(Mapper2.class);
		job2.setCombinerClass(Reducer2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path(intermediateOutputPath));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}
}