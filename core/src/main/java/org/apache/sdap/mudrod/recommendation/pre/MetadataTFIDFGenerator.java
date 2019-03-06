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
package org.apache.sdap.mudrod.recommendation.pre;

import org.apache.sdap.mudrod.discoveryengine.DiscoveryStepAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.recommendation.structure.MetadataTokenizer;
import org.apache.sdap.mudrod.utils.LabeledRowMatrix;
import org.apache.sdap.mudrod.utils.MatrixUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * ClassName: Generate TFIDF information of all metadata
 */
public class MetadataTFIDFGenerator extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(MetadataTFIDFGenerator.class);

  /**
   * Creates a new instance of MatrixGenerator.
   *
   * @param props the Mudrod configuration
   * @param es    the Elasticsearch drive
   * @param spark the spark drive
   */
  public MetadataTFIDFGenerator(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {

    LOG.info("Starting Dataset TF_IDF Matrix Generator");
    startTime = System.currentTimeMillis();
    try {
      generateWordBasedTFIDF();
    } catch (Exception e) {
      LOG.error("Error during Dataset TF_IDF Matrix Generation: {}", e);
    }
    endTime = System.currentTimeMillis();

    LOG.info("Dataset TF_IDF Matrix Generation complete, time elaspsed: {}s", (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  private LabeledRowMatrix generateWordBasedTFIDF() throws Exception {

    MetadataTokenizer opt = new MetadataTokenizer(props);

    String metadataName = props.getProperty(MudrodConstants.METADATA_ID);
    JavaPairRDD<String, String> metadataContents = opt.loadAll(es, spark, metadataName);

    JavaPairRDD<String, List<String>> metadataWords = opt.tokenizeData(metadataContents, " ");

    LabeledRowMatrix wordtfidfMatrix = opt.tFIDFTokens(metadataWords, spark);

    MatrixUtil.exportToCSV(
            wordtfidfMatrix.rowMatrix,
            wordtfidfMatrix.rowkeys,
            wordtfidfMatrix.colkeys,
            props.getProperty(MudrodConstants.METADATA_WORD_MATRIX_PATH));

    return wordtfidfMatrix;
  }

  public LabeledRowMatrix generateTermBasedTFIDF() throws Exception {

    MetadataTokenizer opt = new MetadataTokenizer(props);

    String source = props.getProperty(MudrodConstants.SEMANTIC_FIELDS);
    List<String> variables = new ArrayList<>(Arrays.asList(source.split(",")));

    String metadataName = props.getProperty(MudrodConstants.METADATA_ID);
    JavaPairRDD<String, String> metadataContents = opt.loadAll(es, spark, variables, metadataName);

    JavaPairRDD<String, List<String>> metadataTokens = opt.tokenizeData(metadataContents, ",");

    LabeledRowMatrix tokentfidfMatrix = opt.tFIDFTokens(metadataTokens, spark);

    MatrixUtil.exportToCSV(
            tokentfidfMatrix.rowMatrix,
            tokentfidfMatrix.rowkeys,
            tokentfidfMatrix.colkeys,
            props.getProperty(MudrodConstants.METADATA_TERM_MATRIX_PATH));

    return tokentfidfMatrix;
  }
}
