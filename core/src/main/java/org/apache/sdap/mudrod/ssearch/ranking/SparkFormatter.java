package org.apache.sdap.mudrod.ssearch.ranking;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

public class SparkFormatter {
  private DecimalFormat ndForm;

  public SparkFormatter() {
    NumberFormat nf = NumberFormat.getNumberInstance(Locale.ROOT);
    ndForm = (DecimalFormat) nf;
    ndForm.applyPattern("#.##");
  }

  public void toSparkSVMformat(String inputCSVFileName, String outputTXTFileName) {
    File file = new File(outputTXTFileName);
    if (file.exists()) {
      file.delete();
    }
    try {
      file.createNewFile();

      try (OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(outputTXTFileName), StandardCharsets.UTF_8);
           BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputCSVFileName), StandardCharsets.UTF_8))) {

        String line = null;
        line = br.readLine(); //header
        while ((line = br.readLine())!= null) {
          String[] list = line.split(",");
          String output = "";
          Double label = Double.parseDouble(list[list.length - 1].replace("\"", ""));
          if (label == -1.0) {
            output = "0 ";
          } else if (label == 1.0) {
            output = "1 ";
          }

          for (int i = 0; i < list.length - 1; i++) {
            int index = i + 1;
            output += index + ":" + ndForm.format(Double.parseDouble(list[i].replace("\"", ""))) + " ";
          }
          osw.write(output + "\n");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
