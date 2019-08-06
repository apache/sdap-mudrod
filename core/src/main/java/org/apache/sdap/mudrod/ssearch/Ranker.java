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
package org.apache.sdap.mudrod.ssearch;

import org.apache.sdap.mudrod.discoveryengine.MudrodAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.ssearch.ranking.Learner;
import org.apache.sdap.mudrod.ssearch.structure.SResult;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * Supports the ability to calculating ranking score
 */
public class Ranker extends MudrodAbstract implements Serializable {
  private static final long serialVersionUID = 1L;
  transient List<SResult> resultList = new ArrayList<>();
  Learner le = null;

  public Ranker(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    if("1".equals(props.getProperty(MudrodConstants.RANKING_ML)))
      le = new Learner(spark, props.getProperty(MudrodConstants.RANKING_MODEL));
  }

  /**
   * Method of calculating mean value
   *
   * @param attribute  the attribute name that need to be calculated on
   * @param resultList an array list of result
   * @return mean value
   */
  private double getMean(String attribute, List<SResult> resultList) {
    double sum = 0.0;
    for (SResult a : resultList) {
      sum += (double) SResult.get(a, attribute);
    }
    return getNDForm(sum / resultList.size());
  }

  /**
   * Method of calculating variance value
   *
   * @param attribute  the attribute name that need to be calculated on
   * @param resultList an array list of result
   * @return variance value
   */
  private double getVariance(String attribute, List<SResult> resultList) {
    double mean = getMean(attribute, resultList);
    double temp = 0.0;
    double val;
    for (SResult a : resultList) {
      val = (Double) SResult.get(a, attribute);
      temp += (mean - val) * (mean - val);
    }

    return getNDForm(temp / resultList.size());
  }

  /**
   * Method of calculating standard variance
   *
   * @param attribute  the attribute name that need to be calculated on
   * @param resultList an array list of result
   * @return standard variance
   */
  private double getStdDev(String attribute, List<SResult> resultList) {
    return getNDForm(Math.sqrt(getVariance(attribute, resultList)));
  }

  /**
   * Method of calculating Z score
   *
   * @param val  the value of an attribute
   * @param mean the mean value of an attribute
   * @param std  the standard deviation of an attribute
   * @return Z score
   */
  private double getZscore(double val, double mean, double std) {
    if (!equalComp(std, 0)) {
      return getNDForm((val - mean) / std);
    } else {
      return 0;
    }
  }

  private boolean equalComp(double a, double b) {
    return Math.abs(a - b) < 0.0001;
  }

  /**
   * Get the first N decimals of a double value
   *
   * @param d double value that needs to be processed
   * @return processed double value
   */
  private double getNDForm(double d) {
    NumberFormat nf = NumberFormat.getNumberInstance(Locale.ENGLISH);
    DecimalFormat ndForm = (DecimalFormat) nf;
    ndForm.applyPattern("#.###");
    return Double.valueOf(ndForm.format(d));
  }

  /**
   * Method of ranking a list of result
   *
   * @param resultList result list
   * @return ranked result list
   */
  public List<SResult> rank(List<SResult> resultList) {
    if(le==null) return resultList;
    
    for (int i = 0; i < resultList.size(); i++) {
      for (int m = 0; m < SResult.rlist.length; m++) {
        String att = SResult.rlist[m].split("_")[0];
        double val = SResult.get(resultList.get(i), att);
        double mean = getMean(att, resultList);
        double std = getStdDev(att, resultList);
        double score = getZscore(val, mean, std);
        String scoreId = SResult.rlist[m];
        SResult.set(resultList.get(i), scoreId, score);
      }
    }

    Collections.sort(resultList, new ResultComparator());
    return resultList;
  }
  
  /**
   * Method of comparing results based on final score
   */
  public class ResultComparator implements Comparator<SResult> {
    @Override
    /**
     * @param o1  one item from the search result list
     * @param o2 another item from the search result list
     * @return 1 meaning o1>o2, 0 meaning o1=o2
     */
    public int compare(SResult o1, SResult o2) {
      List<Double> instList = new ArrayList<>();
      for (String str: SResult.rlist) {
        double o2Score = SResult.get(o2, str);
        double o1Score = SResult.get(o1, str);
        instList.add(o2Score - o1Score);
      }

      double[] ins = instList.stream().mapToDouble(i -> i).toArray();
      LabeledPoint insPoint = new LabeledPoint(99.0, Vectors.dense(ins));
      int prediction = (int)le.classify(insPoint);
      
      return prediction;
    }
  }

}
