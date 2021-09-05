package LR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import java.io.*;


public class LRTraining {

    static double[] theta = new double[13];
    static double step;
    static int index_;

    static double hypothesis(double[] X, double[] Theta) {
        double X_sum = 0.0;
        for (int i = 0; i < 13; i++) {
            X_sum += Theta[i] * X[i];
        }
        return 1.0 / (1.0 + Math.exp(-X_sum));
    }

    public static double[] convert(String a) {
        String[] raw_data = a.split(",");
        double[] cpt_data = new double[13];
        for (int i = 0; i < 13; i++) {
            cpt_data[i] = Double.parseDouble(raw_data[i]);
        }
        return cpt_data;
    }

    public static class LRTrainingMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        public void setup(Context context) throws IOException {
            int pre_index = index_ - 1;
            if (pre_index == 0) {
                for (int i = 0; i < 13; i++) {
                    theta[i] = 0.0;
                }
            } else {
                File file = new File("LogisticRegression/ThetaResult"+ pre_index +"/part-r-00000");
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                String[] temp = bufferedReader.readLine().split(",");
                for (int i = 0; i < 13; i++) {
                    theta[i] = Double.parseDouble(temp[i]);
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (!value.toString().equals("-1")) {
                int y = Integer.parseInt(value.toString().split(",")[13]);
                double[] cur_data = convert(value.toString());
                double Sum_pre = hypothesis(cur_data, theta) - y;
                for (int j = 0; j < 13; j++) {
                    cur_data[j] *= Sum_pre;
                    context.write(new Text(String.valueOf(j)), new Text(String.valueOf(cur_data[j])));
                }
            }
        }
    }

    public static class LRTrainingReducer
            extends Reducer<Text, Text, NullWritable, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> value, Context context) {

            double Sum = 0.0;
            int m = 0;

            int j = Integer.parseInt(key.toString());
            for (Text val: value) {
                Sum += Double.parseDouble(val.toString());
                m += 1;
            }
            theta[j] -= step * (Sum / m);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            String[] Output_theta = new String[13];
            for (int j = 0; j < 13; j++) {
                Output_theta[j] = String.valueOf(theta[j]);
            }
            context.write(NullWritable.get(), new Text(String.join(",", Output_theta)));
        }
    }

    public static void mainHelper(int index, String INPUT_PATH) throws IOException, InterruptedException, ClassNotFoundException {
        index_ = index;
        String OUTPUT_PATH="LogisticRegression/ThetaResult" + index;
        BasicConfigurator.configure();
        Path output_path=new Path(OUTPUT_PATH);
        Path input_path=new Path(INPUT_PATH);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LogisticRegression");
        job.setJarByClass(LRTraining.class);
        job.setMapperClass(LRTrainingMapper.class);
        job.setReducerClass(LRTrainingReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input_path);
        FileOutputFormat.setOutputPath(job, output_path);
        job.waitForCompletion(true);
    }

    public static void main(int rounds, double step_input, String INPUT_PATH) throws IOException, InterruptedException, ClassNotFoundException {
        step = step_input;
        int round = 1;
        while (round <= rounds) {
            mainHelper(round, INPUT_PATH);
            round += 1;
        }
        System.out.println("Training Success");
    }
}
