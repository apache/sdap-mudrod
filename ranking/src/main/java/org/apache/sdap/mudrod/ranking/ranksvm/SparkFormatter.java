/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sdap.mudrod.ranking.ranksvm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;

public class SparkFormatter {
  DecimalFormat NDForm = new DecimalFormat("#.###");

  public SparkFormatter() {
  }

  public void toSparkSVMformat(String inputCSVFileName, String outputTXTFileName) {
    File file = new File(outputTXTFileName);
    if (file.exists()) {
      file.delete();
    }
    try {
      file.createNewFile();
      FileWriter fw = new FileWriter(outputTXTFileName);
      BufferedWriter bw = new BufferedWriter(fw);

      BufferedReader br = new BufferedReader(new FileReader(inputCSVFileName));
      br.readLine();
      String line = br.readLine();
      while (line != null) {
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
          output += index + ":" + NDForm.format(Double.parseDouble(list[i].replace("\"", ""))) + " ";
        }
        bw.write(output + "\n");

        line = br.readLine();
      }
      br.close();
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
