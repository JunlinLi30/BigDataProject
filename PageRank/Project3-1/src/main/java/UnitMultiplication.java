import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
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
            // transition mapper
            // read relation.txt
            // from\tto as follow
            // 1\t3,4,5
            // output key = from:1
            // output value = to=prob: 3=1/3
            String line = value.toString().trim();
            String[] fromTo = line.split("\t");

            // 8 ---example fromId = 8
            if(fromTo.length == 1 || fromTo[1].trim().equals("")) { // fromTo.length < 2 没有跳转网页---deadends ||之后的条件可以忽略？
                // 真正工作时：
                // exception --- bad input --- but 是否是真正的exception，因为edge case也有可能出现这样（deadend， spider trap）（不能当作exception）
                // 输出一下logger： logger.debug("found dead ends:" 8---dead ends 数？)
                return;
            }
            String from = fromTo[0];
            String[] tos = fromTo[1].split(",");
            for (String to: tos) {
                context.write(new Text(from), new Text(to + "=" + (double)1/tos.length));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {//接收pr值

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // read input : 1\t1
            String[] pr = value.toString().trim().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));// (ID, weight)
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //key = fromId
            //value = <toId=prob, toId=prob...pr(from 网页的pr值)> <2=1/3, 5=1/3, 8=1/3>
            //outputKey = toId
            //outputValue = prob * pr = subPr
            List<String> transitionUnit = new ArrayList<String>();
            double prUnit = 0;//prCell = 0
            for (Text value: values) {
                if(value.toString().contains("=")) { //判断是prob值
                    transitionUnit.add(value.toString());
                }
                else { //判断是pr值
                    prUnit = Double.parseDouble(value.toString());
                }
            }
            for (String unit: transitionUnit) {
                //transCell: toId=prob 2=1/3
                String outputKey = unit.split("=")[0];
                double relation = Double.parseDouble(unit.split("=")[1]);
                String outputValue = String.valueOf(relation * prUnit);
                context.write(new Text(outputKey), new Text(outputValue));// toId, subPr of each toId ==> toId\tsubPr (MP默认之间用\t间隔)
                // doublewritable 也行，反正写进去hdfs文件也是string形式
                // 在driver 要额外定义doublewritable是什么，因为和 mapper后面text text格式不一致
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        // 与之前的MR差不多
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 与之前有所区别在这里：map一下input到不同的mapper里面
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);//把读进来的路径map到需要的class（格式）

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
