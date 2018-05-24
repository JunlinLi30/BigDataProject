import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //key is just a 偏移量，在文中?
            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability

            // transition mapper
            // read relation.txt
            // from\tto as follow
            // 1\t3,4,5
            // output key = from:1
            // output value = to=prob: 3=1/3  --- initially they are equally same

            String line = value.toString().trim();
            String[] fromTo = line.split("\t");

            if (fromTo.length < 2 || fromTo[1].trim().equals("")) {
                return;
            }

            String from = fromTo[0];
            String[] tos = fromTo[1].split(",");
            for(String to : tos){
                context.write(new Text(from), new Text(to + "=" + (double)1/tos.length));
            }

        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer
            // (ID, weight)
            String[] pr = value.toString().trim().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));

        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        float beta; // why float

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
            //key = fromId
            //value = <toId=prob, toId=prob...pr(from 网页的pr值)> <2=1/3, 5=1/3, 8=1/3, pr>
            //outputKey = toId
            //outputValue = prob * pr = subPr
//            String fromId = key.toString().trim(); //what's the use of key?
            List<String> transitionUnit = new ArrayList<String>();
            double prUnit = 0;
            for (Text value : values) {
                String val = value.toString().trim();
                if (val.contains("=")) { //与pr值区别开
                    transitionUnit.add(val);
                } else {
                    prUnit = Double.parseDouble(val);
                }
            }
            // 竖着列乘以列（乘一次就写出，然后再另一个java里面用mr把ID相同的（比如都是第一行（某ID））sum起来）
            for (String each : transitionUnit) {
                String[] info = each.trim().split("=");
                String outputKey = info[0];
                double relation = Double.parseDouble(info[1]);
                String outputValue = String.valueOf(relation * prUnit * (1 - beta));
                context.write(new Text(outputKey), new Text(outputValue));
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.getFloat("beta", Float.parseFloat(args[3])); // one more argument for beta;
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
