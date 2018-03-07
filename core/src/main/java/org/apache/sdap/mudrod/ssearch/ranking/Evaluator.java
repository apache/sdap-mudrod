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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Supports ability to evaluating ranking results
 */
public class Evaluator {
  /**
   * Method of calculating NDCG score
   *
   * @param list a list of integer with each integer element indicating
   *             the performance at its position
   * @param k    the number of elements needed to be included in the calculation
   * @return NDCG score
   */
  public double getNDCG(int[] list, int k) {
    double dcg = this.getDCG(list, k);
    double idcg = this.getIDCG(list, k);
    double ndcg = 0.0;
    if (idcg > 0.0) {
      ndcg = dcg / idcg;
    }
    return ndcg;
  }

  /**
   * Method of getting the precision of a list at position K
   *
   * @param list a list of integer with each integer element indicating
   *             the performance at its position
   * @param k    the number of elements needed to be included in the calculation
   * @return precision at K
   */
  public double getPrecision(int[] list, int k) {
    int size = list.length;
    if (size == 0 || k == 0) {
      return 0;
    }

    if (k > size) {
      k = size;
    }

    int relDocNum = this.getRelevantDocNum(list, k);
    return (double) relDocNum / (double) k;
  }

  /**
   * Method of getting the number of relevant element in a ranking results
   *
   * @param list a list of integer with each integer element indicating
   *             the performance at its position
   * @param k    the number of elements needed to be included in the calculation
   * @return the number of relevant element
   */
  private int getRelevantDocNum(int[] list, int k) {
    int size = list.length;
    if (size == 0 || k == 0) {
      return 0;
    }

    if (k > size) {
      k = size;
    }

    int relNum = 0;
    for (int i = 0; i < k; i++) {
      if (list[i] > 3) { // 3 refers to "OK"
        relNum++;
      }
    }
    return relNum;
  }

  /**
   * Method of calculating DCG score from a list of ranking results
   *
   * @param list a list of integer with each integer element indicating
   *             the performance at its position
   * @param k    the number of elements needed to be included in the calculation
   * @return DCG score
   */
  private double getDCG(int[] list, int k) {
    int size = list.length;
    if (size == 0 || k == 0) {
      return 0.0;
    }

    if (k > size) {
      k = size;
    }

    double dcg = list[0];
    for (int i = 1; i < k; i++) {
      int rel = list[i];
      int pos = i + 1;
      double relLog = Math.log(pos) / Math.log(2);
      dcg += rel / relLog;
    }
    return dcg;
  }

  /**
   * Method of calculating ideal DCG score from a list of ranking results
   *
   * @param list a list of integer with each integer element indicating
   *             the performance at its position
   * @param k    the number of elements needed to be included in the calculation
   * @return IDCG score
   */
  private double getIDCG(int[] list, int k) {
    Comparator<Integer> comparator = new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return o2.compareTo(o1);
      }
    };
    List<Integer> sortlist = IntStream.of(list).boxed().collect(Collectors.toList());
    Collections.sort(sortlist, comparator);
    int[] sortedArr = sortlist.stream().mapToInt(i -> i).toArray();
    return this.getDCG(sortedArr, k);
  }

}
