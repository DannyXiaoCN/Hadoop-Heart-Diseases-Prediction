package LR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class DataReverse {

    public static class DReverseMapper
            extends Mapper<Object, Text, NullWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), value);
        }
    }

    public static void main(String INPUT_PATH, String OUTPUT_PATH) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();
        Path output_path=new Path(OUTPUT_PATH);
        Path input_path=new Path(INPUT_PATH);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "数据倒置");
        job.setJarByClass(DataReverse.class);
        job.setMapperClass(DReverseMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input_path);
        FileOutputFormat.setOutputPath(job, output_path);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}