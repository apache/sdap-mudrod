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
package org.apache.sdap.mudrod.ssearch.ranking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

public class SparkFormatter {
 private DecimalFormat ndForm;
 private static final Logger LOG = LoggerFactory.getLogger(SparkFormatter.class);

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
      LOG.error("Writing to output text file failed : ", e);
    }
  }
}
