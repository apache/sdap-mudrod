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
package org.apache.sdap.mudrod.utils;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for Similarity Utility class. {@link org.apache.sdap.mudrod.utils.SimilarityUtil}
 */
public class TestSimilarityUtils {

  @Test
  public final void testCosineSimilarity() {

    // Vector A :[2, 2, 3, 5]
    // Vector B :[4, 1, 2, 1]

    double[] vecAValues = {2, 2, 3, 5};
    double[] vecBValues = {4, 1, 2, 1};
    Vector vecA = Vectors.dense(vecAValues);
    Vector vecB = Vectors.dense(vecBValues);
    double cosineDistance = SimilarityUtil.cosineDistance(vecA, vecB);
    double expectedValue = 0.6908492797077574;
    Assert.assertEquals("Cosine similarity calculation failed for 4-D vectors.",
            new Double(expectedValue), new Double(cosineDistance));

    // Vector C :[1, 2]
    // Vector D :[4, 5]

    double[] vecCValues = {1, 2};
    double[] vecDValues = {4, 5};
    Vector vecC = Vectors.dense(vecCValues);
    Vector vecD = Vectors.dense(vecDValues);
    cosineDistance = SimilarityUtil.cosineDistance(vecC, vecD);
    expectedValue = 0.9778024140774094;
    Assert.assertEquals("Cosine similarity calculation failed for 2-D vectors.",
            new Double(expectedValue), new Double(cosineDistance));

    // Vector E :[5, 2, 6]
    // Vector F :[4, 5, 7]

    double[] vecEValues = {5, 2, 6};
    double[] vecFValues = {4, 5, 7};
    Vector vecE = Vectors.dense(vecEValues);
    Vector vecF = Vectors.dense(vecFValues);
    cosineDistance = SimilarityUtil.cosineDistance(vecE, vecF);
    expectedValue = 0.9413574486632833;
    Assert.assertEquals("Cosine similarity calculation failed for 3-D vectors.",
            new Double(expectedValue), new Double(cosineDistance));
  }

}
