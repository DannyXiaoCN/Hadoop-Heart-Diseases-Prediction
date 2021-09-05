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
import java.io.*;
import static LR.LRTraining.hypothesis;
import static LR.LRTraining.convert;

public class LRPredict {

    static double correct_num = 0;
    static double all_num = 0;
    static double[] theta = new double[13];
    static int index_;

    public static class LRPredictMapper
            extends Mapper<Object, Text, NullWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (value.toString().equals("-1")) {
                context.write(NullWritable.get(), new Text("-1"));
            } else {
                String[] temp = value.toString().split(",");
                int y = -1;
                if (temp.length == 14) {
                    y = Integer.parseInt(temp[13]);
                }
                double[] cur_data = convert(value.toString());

                if (hypothesis(cur_data, theta) > 0.5) {
                    context.write(NullWritable.get(), new Text(String.valueOf(1)));
                    if (y == 1) {
                        correct_num += 1;
                    }
                } else {
                    context.write(NullWritable.get(), new Text(String.valueOf(0)));
                    if (y == 0) {
                        correct_num += 1;
                    }
                }
                all_num += 1;
            }
        }
    }

    public static int main(String INPUT_PATH, String OUTPUT_PATH, int index, int mode) throws IOException, InterruptedException, ClassNotFoundException {

        index_ = index;
        File file = new File("LogisticRegression/ThetaResult"+ index_ +"/part-r-00000");
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String[] temp = bufferedReader.readLine().split(",");
        for (int i = 0; i < 13; i++) {
            theta[i] = Double.parseDouble(temp[i]);
        }
        if (mode == 2) {
            BasicConfigurator.configure();
            Path output_path=new Path(OUTPUT_PATH);
            Path input_path=new Path(INPUT_PATH);
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "LogisticRegressionPrediction");
            job.setJarByClass(LRPredict.class);
            job.setMapperClass(LRPredictMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, input_path);
            FileOutputFormat.setOutputPath(job, output_path);
            job.waitForCompletion(true);
            BufferedWriter result = new BufferedWriter(new FileWriter("LogisticRegression/PredictionResult.txt", true));
            result.write(correct_num / all_num + "\n");
            result.close();
            return -1;
        } else {
            // 单条预测
            double[] cur_data = convert(INPUT_PATH);
            if (hypothesis(cur_data, theta) > 0.5) {
                System.out.println("PREDICTION SUCCESS");
                return 1;
            } else {
                return 0;
            }
        }
    }
}

