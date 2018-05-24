import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //movieA:movieB \t relation
            //input movieA:movieB\trelation
            //outputKey = movieA
            //outputValue = movieB=relation
            //collect the relationship list for movieA
            String[] movie_relation = value.toString().trim().split("\t");
            String[] movies = movie_relation[0].split(":");

            context.write(new Text(movies[0]), new Text(movies[1] + "=" + movie_relation[1]));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //key = movieA, value=<movieB:relation1, movieC:relation2...>
            //sum(relation) = denominator
            //outputKey = movieB
            //outputValue = (movieA = relation1/denominator) 转置了，因为要按列来输出
            int sum = 0;
            Map<String, Integer> map = new HashMap<String, Integer>();// movie_relation_map
            while (values.iterator().hasNext()) { //for(Text value : values) {}
                //value: movieB=relation
                String[] movie_relation = values.iterator().next().toString().split("=");
                int relation = Integer.parseInt(movie_relation[1]);
                sum += relation;
                map.put(movie_relation[0], relation);
            }
            //准备写出
            //iterate map: Iterator iterator = map.entrySet().iterator();
            // while (iterator.hasNext()) { Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iterator.next();}
            for(Map.Entry<String, Integer> entry: map.entrySet()) {
                //key = movieB
                //value = movieA=relation/denominator
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + (double)entry.getValue()/sum; // key 为reducer收到的key = movieA
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
