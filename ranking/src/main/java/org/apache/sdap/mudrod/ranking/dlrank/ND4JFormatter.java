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
package org.apache.sdap.mudrod.ranking.dlrank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import au.com.bytecode.opencsv.CSVWriter;

public class ND4JFormatter {
  DecimalFormat NDForm = new DecimalFormat("#.###");

  public ND4JFormatter() {
  }

  public void toND4Jformat(String inputCSVFileName, String outputCSVFileName) {
    File file = new File(outputCSVFileName);
    if (file.exists()) {
      file.delete();
    }

    try (BufferedReader br = new BufferedReader(new FileReader(inputCSVFileName)); CSVWriter csvOutput = new CSVWriter(new FileWriter(outputCSVFileName), ',');) {
      String line = br.readLine();
      while (line != null) {
        String[] list = line.split(",");
        String label = list[list.length - 1].replace("\"", "");
        String output = label;
        if (label.equals("-1") || label.equals("-1.0")) {
          output = "0";
        } else if (label.equals("1") || label.equals("1.0")) {
          output = "1";
        }

        for (int i = list.length - 1; i > 0; i--) {
          list[i] = list[i - 1].replace("\"", "");
        }
        list[0] = output;

        csvOutput.writeNext(list); // Write this array to the file
        line = br.readLine();
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
