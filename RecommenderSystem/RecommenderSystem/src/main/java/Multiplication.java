import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB\tmovieA=relation
			// movieB 作为key， movieA=relation作为value输出就可以了

			String[] line = value.toString().split("\t");
			context.write(new Text(line[0]), new Text(line[1]));

		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input: userId,movieId,rating
			//output: movieId---key  userId:rating
			String[] line = value.toString().split(",");
			context.write(new Text(line[1]), new Text(line[0] + ":" + line[2]));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
			//key = movieId
			//value = <movie1=relation1, movie2=relation2...user1:rating1, user2:rating2...>
			//Map1<movieId, relation>
			//Map2<userId, rating>
			// key = user:movie
			// outputvalue = (subrating) relation * rating
			Map<String, Double> relationMap = new HashMap<String, Double>();
			Map<String, Double> ratingMap = new HashMap<String, Double>();

			for (Text value: values) {
				if(value.toString().contains("=")) { //区别数据从哪个mapper来的，reducer只懂得收集数据，不知道数据从哪儿来
					String[] movie_relation = value.toString().split("=");
					relationMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
				}
				else {
					String[] user_rating = value.toString().split(":");
					ratingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
				}
			}

			for(Map.Entry<String, Double> entry: relationMap.entrySet()) {
				String movie = entry.getKey();
				double relation = entry.getValue();

				for(Map.Entry<String, Double> element: ratingMap.entrySet()) {
					String user = element.getKey();
					double rating = element.getValue();
					context.write(new Text(user + ":" + movie), new DoubleWritable(relation*rating));//outputKey, outputValue
					// e.g. one subrating: user1:movie1 2/6*7.5
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
