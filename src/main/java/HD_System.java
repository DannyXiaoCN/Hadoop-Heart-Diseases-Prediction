import LR.DataReverse;
import LR.LRPredict;
import LR.LRTraining;

import javax.sound.sampled.Line;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class HD_System {

    private static Scanner sc;
    private static ArrayList<String> Info_list;

    private static void SystemStart(String DEFAULT_ADDR) throws InterruptedException {
        // 函数启动部分
        System.out.println("Welcome to use the Heart Disease Analyse System");
        TimeUnit.MILLISECONDS.sleep(200);
        System.out.println("Please select the Model Training method");
        TimeUnit.MILLISECONDS.sleep(200);
        System.out.println("Please type 1 to enter the DEFAULT mode");
        TimeUnit.MILLISECONDS.sleep(200);
        System.out.println("Please type 2 to enter the PERSONAL mode");
        TimeUnit.MILLISECONDS.sleep(200);
        sc = new Scanner(System.in);
        Info_list = new ArrayList<>();
        int flag = 0;
        while (true) {
            String mode_choice = sc.nextLine();
            if (mode_choice.equals("1")) {
                System.out.println("Now entering [DEFAULT] mode");
                flag = 1;
                break;
            } else if (mode_choice.equals("2")) {
                System.out.println("[PERSONAL] mode has been chosen. Please enter the FILE ADDRESS");
                flag = 2;
                break;
            } else {
                System.out.println("[ILLEGAL INPUT]. Please read the INSTRUCTION");
            }
        }
        if (flag == 2) {
            Info_list.add(sc.nextLine());
        } else {
            Info_list.add(DEFAULT_ADDR);
        }
        System.out.println("Please enter the TRAINING ROUNDS");
        Info_list.add(sc.nextLine());
        System.out.println("Please enter the LEARNING RATE (0.01, 0.1, ...)");
        Info_list.add(sc.nextLine());
        System.out.println("Please select the Predict MODE");
        TimeUnit.MILLISECONDS.sleep(200);
        System.out.println("Please type 1 to enter the SINGLE mode");
        TimeUnit.MILLISECONDS.sleep(200);
        System.out.println("Please type 2 to enter the FILE(large) mode");
        TimeUnit.MILLISECONDS.sleep(200);
        String mode = sc.nextLine();
        while (true) {
            if (mode.equals("1")) {
                System.out.println("Please input your data");
                TimeUnit.MILLISECONDS.sleep(200);
                System.out.println("FORMAT: age,sex,cp,trestbps,chol,fbs,restecg,thalach,exang,oldpeak,slope,ca,thal");
                Info_list.add(sc.nextLine());
                System.out.println("TYPE 1 FOR VALIDATION");
                Info_list.add(sc.nextLine());
                sc.close();
                break;
            } else if (mode.equals("2")) {
                System.out.println("Please input your INPUT FILE ADDR");
                Info_list.add(sc.nextLine());
                TimeUnit.MILLISECONDS.sleep(200);
                System.out.println("Please input your Desired OUPUT FILE ADDR");
                Info_list.add(sc.nextLine());
                System.out.println("TYPE 1 FOR VALIDATION");
                Info_list.add(sc.nextLine());
                sc.close();
                return;
            } else {
                System.out.println("[ILLEGAL INPUT]. Please read the INSTRUCTION");
            }
        }
    }

    private static void Model_Training(String FILE_ADDR) throws IOException, InterruptedException, ClassNotFoundException {
        LRTraining.main(Integer.parseInt(Info_list.get(1)), Double.parseDouble(Info_list.get(2)), FILE_ADDR);
    }

    private static void Prediction(int RESULT_INDX, String MID_ADDR) throws InterruptedException, IOException, ClassNotFoundException {
        if (Info_list.size() == 5) {
            if (HD_DataPreProcess.DataFilter(Info_list.get(3).split(","))) {
                String pp_data = HD_DataPreProcess.main(Info_list.get(3), "", 2, 100);
                int PRED_RESULT = LRPredict.main(pp_data, "", RESULT_INDX, 1);
                if (PRED_RESULT == 1) {
                    System.out.println("YOUR RESULT IS YES. YOU NEED TO GO TO HOSPITAL");
                } else {
                    System.out.println("YOUR RESULT IS NO. YOU ARE HEALTHY");
                }
            } else {
                System.out.println("YOUR DATA CANNOT BE PROCESSED");
            }
        } else {
            // Test Mode 之后加 valid Mode
            HD_DataPreProcess.main(Info_list.get(3), "hdfs://localhost:9000/PP/TestData", 1, 100);
            LRPredict.main("hdfs://localhost:9000/PP/TestData/part-r-00000", MID_ADDR, RESULT_INDX, 2);
            DataReverse.main(MID_ADDR, Info_list.get(4));
        }
    }

    private static void Validation(int RESULT_INDX, String MID_ADDR) throws IOException, InterruptedException, ClassNotFoundException {
        HD_DataPreProcess.main(Info_list.get(3), "hdfs://localhost:9000/PP/TestData", 3, 100);
        LRPredict.main("hdfs://localhost:9000/PP/TestData/part-r-00000", MID_ADDR, RESULT_INDX, 2);
        DataReverse.main(MID_ADDR, Info_list.get(4));
    }

    private static void SystemEnd() {
        System.out.println("JOB DONE. SYSTEM CLOSED");
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        SystemStart("heart.txt");
        // 数据预处理部分
        HD_DataPreProcess.main(Info_list.get(0), "hdfs://localhost:9000/PP/TrainData", 0, 50000);
        Model_Training("hdfs://localhost:9000/PP/TrainData/part-r-00000");
        String MID_ADDR = "hdfs://localhost:9000/PP/LogisticRegression/outputPre";

        if (Info_list.get(Info_list.size() - 1).equals("1")) {
            Validation(Integer.parseInt(Info_list.get(1)), MID_ADDR);
        } else {
            Prediction(Integer.parseInt(Info_list.get(1)), MID_ADDR);
        }

        SystemEnd();
    }
}
