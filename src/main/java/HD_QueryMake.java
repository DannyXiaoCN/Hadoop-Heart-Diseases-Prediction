
import java.io.*;
import java.util.ArrayList;
import java.util.Random;

public class HD_QueryMake {


    public static void main(String[] args) throws IOException {

        BufferedWriter bw = new BufferedWriter(new FileWriter("random_query.txt"));
        Random r = new Random();
        Random r2 = new Random();
        ArrayList<Integer> MAX = new ArrayList<>();
        ArrayList<Integer> MIN = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("MAX_MIN.txt"));
        String[] temp = br.readLine().split(",");
        for (int i = 0; i < 13; i++) {
            MAX.add((int)Double.parseDouble(temp[i]));
        }
        temp = br.readLine().split(",");
        for (int i = 0; i < 13; i++) {
            MIN.add((int)Double.parseDouble(temp[i]));
        }
        for (int i = 0; i < 1000000; i++) {
            String[] temptemp = new String[20];
            for (int k = 0; k < 13; k++) {
                if (k == 8) {
                    temptemp[8] = String.valueOf(r2.nextDouble() * 8);
                }
                temptemp[k] = String.valueOf(r.nextInt(MAX.get(k) - MIN.get(k)) + MIN.get(k));
            }
            for (int j = 13; j < 20; j++) {
                temptemp[j] = String.valueOf(r.nextInt(400));
            }
            bw.write(String.join(",", temptemp) + "\n");
        }
        bw.close();
    }

}
