import org.apache.commons.lang3.StringUtils;
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
import java.util.ArrayList;
import java.util.Random;


public class HD_DataPreProcess {


    static ArrayList<Double> MAX;
    static ArrayList<Double> MIN;

    static int ratio_;




    static int mode_;
    public static boolean DataFilter(String[] value) {
        // 分别对 age 范围：1到100
        return (Integer.parseInt(value[0]) <= 100 && Integer.parseInt(value[0]) >= 1) &&
                (value[1].equals("1") || value[1].equals("0")) &&
                (value[2].equals("0") || value[2].equals("1") || value[2].equals("2") || value[2].equals("3")) &&
                (Integer.parseInt(value[3]) <= 1000 && Integer.parseInt(value[3]) >= 0) &&
                (Integer.parseInt(value[4]) <= 1000) && Integer.parseInt(value[4]) >= 0 &&
                (value[5].equals("1") || value[5].equals("0")) &&
                (value[6].equals("1") || value[6].equals("0") || value[6].equals("2")) &&
                (Integer.parseInt(value[7]) <= 500 && Integer.parseInt(value[7]) >= 0) &&
                (value[8].equals("1") || value[8].equals("0")) &&
                Double.parseDouble(value[9]) >= 0 &&
                (value[10].equals("1") || value[10].equals("2") || value[10].equals("0")) &&
                (value[11].equals("0") || value[11].equals("1") || value[11].equals("2") || value[11].equals("3") || value[11].equals("4")) &&
                (value[12].equals("0") || value[12].equals("1") || value[12].equals("2") || value[12].equals("3"));
    }

    public static class PPMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 首先对age进行奇异值过滤
            int age = Integer.parseInt(value.toString().split(",")[0]);
            if (DataFilter(value.toString().split(","))) {
                // 将超出范围的age数据过滤
                String[] norm_data = new String[0];
                if (mode_ == 0 || mode_ == 3) {
                    // 训练模式 数据=14
                    String[] data = value.toString().split(",");
                    if (data.length >= 14) {
                        // 同时过滤少于14列的数据，直接丢弃
                        // 将多余的列去除
                        norm_data = new String[14];
                        System.arraycopy(data, 0, norm_data, 0, 14);
                    }
                } else {
                    // 测试模式 数据=13
                    String[] data = value.toString().split(",");
                    if (data.length >= 13) {
                        // 同时过滤少于13列的数据，直接丢弃
                        // 将多余的列去除
                        norm_data = new String[13];
                        System.arraycopy(data, 0, norm_data, 0, 13);
                    }
                }
                if (mode_ == 0) {
                    for (int i = 0; i < 13; i++) {
                        double temp = Double.parseDouble(norm_data[i]);
                        if (temp > MAX.get(i)) {
                            MAX.set(i, temp);
                        }
                        if (temp < MIN.get(i)) {
                            MIN.set(i, temp);
                        }
                    }
                }
                context.write(new Text(age + "," + norm_data[4]), new Text(String.join(",", norm_data)));
            } else {
                context.write(new Text("-1"), new Text("-1"));
            }
        }
    }


    public static class PPReducer extends Reducer<Text, Text, NullWritable, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            Random rand = new Random();
            if (key.toString().equals("-1")) {
                for (Text val : value) {
                    context.write(NullWritable.get(), val);
                }
            } else {
                for (Text val : value) {
                    if (rand.nextInt(ratio_) < 100) {
                        // 抽样比 1000：1
                        double[] cur_data = new double[13];
                        String[] cur_str = val.toString().split(",");
                        for (int i = 0; i < 13; i++) {
                            cur_data[i] = Double.parseDouble(cur_str[i]);
                            cur_data[i] = (cur_data[i] - MIN.get(i)) / (MAX.get(i) - MIN.get(i));
                            cur_str[i] = String.valueOf(cur_data[i]);
                        }
                        context.write(NullWritable.get(), new Text(String.join(",", cur_str)));
                    }
                }
            }
        }
    }

    public static String main(String INPUT_PATH, String OUTPUT_PATH, int mode, int ratio) throws IOException, InterruptedException, ClassNotFoundException {

        ratio_ = ratio;
        mode_ = mode;
        MAX = new ArrayList<>();
        MIN = new ArrayList<>();
        if (mode_ != 2) {
            if (mode_ != 0) {
                BufferedReader br = new BufferedReader(new FileReader("MAX_MIN.txt"));
                String[] temp = br.readLine().split(",");
                for (int i = 0; i < 13; i++) {
                    MAX.add(Double.parseDouble(temp[i]));
                }
                temp = br.readLine().split(",");
                for (int i = 0; i < 13; i++) {
                    MIN.add(Double.parseDouble(temp[i]));
                }
                br.close();
            } else {
                for (int i = 0; i < 13; i++) {
                    MAX.add(Double.MIN_VALUE);
                    MIN.add(Double.MAX_VALUE);
                }
            }
            BasicConfigurator.configure();
            Path output_path=new Path(OUTPUT_PATH);
            Path input_path=new Path(INPUT_PATH);
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "数据预处理");
            job.setJarByClass(HD_DataPreProcess.class);
            job.setMapperClass(PPMapper.class);
            job.setReducerClass(PPReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, input_path);
            FileOutputFormat.setOutputPath(job, output_path);
            job.waitForCompletion(true);
            if (mode_ == 0) {
                BufferedWriter bw = new BufferedWriter(new FileWriter("MAX_MIN.txt"));
                for (int i = 0; i < 12; i++) {
                    bw.write(MAX.get(i) + ",");
                }
                bw.write(MAX.get(12) + "\n");
                for (int i = 0; i < 12; i++) {
                    bw.write(MIN.get(i) + ",");
                }
                bw.write(MIN.get(12) + "\n");
                bw.close();
            }
            return "";
        } else {
            BufferedReader br = new BufferedReader(new FileReader("MAX_MIN.txt"));
            String[] temp = br.readLine().split(",");
            for (int i = 0; i < 13; i++) {
                MAX.add(Double.parseDouble(temp[i]));
            }
            temp = br.readLine().split(",");
            for (int i = 0; i < 13; i++) {
                MIN.add(Double.parseDouble(temp[i]));
            }
            br.close();
            String[] data_Str = INPUT_PATH.split(",");
            double[] cur_data = new double[13];
            for (int i = 0; i < 13; i++) {
                cur_data[i] = Double.parseDouble(data_Str[i]);
                cur_data[i] = (cur_data[i] - MIN.get(i)) / (MAX.get(i) - MIN.get(i));
                data_Str[i] = String.valueOf(cur_data[i]);
            }
            return String.join(",", data_Str);
        }
    }
}
