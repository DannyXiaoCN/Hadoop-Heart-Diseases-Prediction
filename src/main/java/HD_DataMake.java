import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class HD_DataMake {


    public static void main(String[] args) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader("heart.txt"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("mess_train.txt"));
        Random r = new Random();
        String temp_str = br.readLine();
        while (temp_str != null) {
            for (int i = 0; i < 500; i++) {
                String[] temptemp = new String[20];
                bw.write(temp_str);
                bw.write("," + r.nextInt(400));
                bw.write("," + r.nextInt(400));
                bw.write("," + r.nextInt(400));
                bw.write("," + r.nextInt(400));
                bw.write("\n");
            }
            temp_str = br.readLine();
        }
        br.close();
        bw.close();
    }

}
