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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.ranking.common.RankLearner;
import org.apache.sdap.mudrod.ranking.common.LearnerFactory;
import org.apache.sdap.mudrod.ranking.ranksvm.RankSVMLearner;
import org.apache.sdap.mudrod.ranking.ranksvm.SparkFormatter;
import org.apache.sdap.mudrod.ranking.traindata.RankTrainDataFactory;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.split.FileSplit;
import org.datavec.api.util.ClassPathResource;
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;

public class DLRankLearner extends RankLearner {
  
  MultiLayerNetwork model = null;
  
  /**
   * Constructor to train rank model with deep learning method
   *
   * @param classifierName
   *          classifier type
   * @param skd
   *          an instance of spark driver
   * @param svmSgdModel
   *          path to a trained model
   */
  public DLRankLearner(Properties props, ESDriver es, SparkDriver spark, String dlModel) {
    super(props, es, spark);
    load(dlModel);
  }

  @Override
  public String customizeData(String sourceDir, String outFileName) {
    RankTrainDataFactory factory = new RankTrainDataFactory(props, es, spark);
    String resultFile = factory.createRankTrainData("experts", sourceDir);
    
    String path = new File(resultFile).getParent();
    String separator = System.getProperty("file.separator");
    String nd4jFile = path + separator + outFileName + ".csv";
    ND4JFormatter sf = new ND4JFormatter();
    sf.toND4Jformat(resultFile, nd4jFile);
    return nd4jFile;
  }

  @Override
  public void train(String trainFile) {
    //init model
    if(model == null){
      
      int seed = 123;
      double learningRate = 0.01;
      int numInputs = 7;
      int numOutputs = 2;
      int numHiddenNodes = 20;
      MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
              .seed(seed)
              .updater(new Nesterovs(learningRate, 0.9))
              .list()
              .layer(0, new DenseLayer.Builder().nIn(numInputs).nOut(numHiddenNodes)
                      .weightInit(WeightInit.XAVIER)
                      .activation(Activation.RELU)
                      .build())
              .layer(1, new OutputLayer.Builder(LossFunction.NEGATIVELOGLIKELIHOOD)
                      .weightInit(WeightInit.XAVIER)
                      .activation(Activation.SOFTMAX)
                      .nIn(numHiddenNodes).nOut(numOutputs).build())
              .pretrain(false).backprop(true).build();

      model = new MultiLayerNetwork(conf);
      model.init();
    }
 
    //Load the training data
    int numLinesToSkip = 1;
    char delimiter = ',';
    RecordReader rr = new CSVRecordReader(numLinesToSkip,delimiter);
    try {
      rr.initialize(new FileSplit(new File(trainFile)));
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
    
    //train model
    int batchSize = 50;
    int nEpochs = 30;
    DataSetIterator trainIter = new RecordReaderDataSetIterator(rr,batchSize,0,2);
    for ( int n = 0; n < nEpochs; n++) {
        model.fit( trainIter );
    }
  }

  @Override
  public void evaluate(String testFile) {
    //Load the test/evaluation data:
    int batchSize = 50;
    int numOutputs = 2;
    int numLinesToSkip = 1;
    char delimiter = ',';
    RecordReader rrTest = new CSVRecordReader(numLinesToSkip,delimiter);
    try {
      rrTest.initialize(new FileSplit(new File(testFile)));
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
    DataSetIterator testIter = new RecordReaderDataSetIterator(rrTest,batchSize,0,2);
 
    System.out.println("Evaluate model....");
    Evaluation eval = new Evaluation(numOutputs);
    while(testIter.hasNext()){
        DataSet t = testIter.next();
        INDArray features = t.getFeatureMatrix();
        INDArray lables = t.getLabels();
        INDArray predicted = model.output(features,false);
        eval.eval(lables, predicted);
    }
    System.out.println(eval.stats());
  }

  @Override
  public double predict(double[] value) {
    int nRows = 1;
    int nColumns = value.length;
    INDArray features = Nd4j.zeros(nRows, nColumns);
    for(int i=0; i<nColumns; i++){
      features = features.putScalar(i, value[i]);
    }
    INDArray predicted = model.output(features,false);
    return predicted.getDouble(0);
  }

  @Override
  public void save() {
    // Save model
    String modelPath = RankSVMLearner.class.getClassLoader().getResource("javaDLRankModel").toString();
    File modelFile = new File(modelPath);
    try {
      model.save(modelFile);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void load(String dlRankModel) {
    if(dlRankModel == ""){
      return;
    }
    File modelFile = new File(dlRankModel);
    try {
      model = MultiLayerNetwork.load(modelFile, true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] arg0) {
    MudrodEngine me = new MudrodEngine();
    Properties props = me.loadConfig();

    SparkDriver spark = new SparkDriver(props);
    ESDriver es = new ESDriver(props);

    LearnerFactory factory = new LearnerFactory(props, es, spark);
    RankLearner le = factory.createLearner();

    String trainDir = "E://data//mudrod//ranking//rankingResults//training//training_data_v4";
    String trainFile = le.customizeData(trainDir, "TrainDataForND4J");
    le.train(trainFile);
    //le.save();
    
    String testDir = "E://data//mudrod//ranking//rankingResults//training//test_data_v4";
    String testile = le.customizeData(trainDir, "TestDataForND4J");
    le.evaluate(testile);
  }
}
