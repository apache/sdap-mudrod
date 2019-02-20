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
package org.apache.sdap.mudrod.recommendation.structure;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class MetadataFeature implements Serializable {

  private static final long serialVersionUID = 1L;

  protected static final Integer VAR_SPATIAL = 1;
  protected static final Integer VAR_TEMPORAL = 2;
  protected static final Integer VAR_CATEGORICAL = 3;
  protected static final Integer VAR_ORDINAL = 4;

  public Map<String, Integer> featureTypes = new HashMap<>();
  public Map<String, Integer> featureWeights = new HashMap<>();

  public void normalizeMetadataVariables(Map<String, Object> metadata, Map<String, Object> updatedValues) {

    this.normalizeSpatialVariables(metadata, updatedValues);
    this.normalizeTemporalVariables(metadata, updatedValues);
    this.normalizeOtherVariables(metadata, updatedValues);
  }

  public void inital() {
    this.initFeatureType();
    this.initFeatureWeight();
  }

  public void featureSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder) {
    this.spatialSimilarity(metadataA, metadataB, contentBuilder);
    this.temporalSimilarity(metadataA, metadataB, contentBuilder);
    this.categoricalVariablesSimilarity(metadataA, metadataB, contentBuilder);
    this.ordinalVariablesSimilarity(metadataA, metadataB, contentBuilder);
  }

  /* for normalization */
  public abstract void normalizeSpatialVariables(Map<String, Object> metadata, Map<String, Object> updatedValues);

  public abstract void normalizeTemporalVariables(Map<String, Object> metadata, Map<String, Object> updatedValues);

  public abstract void normalizeOtherVariables(Map<String, Object> metadata, Map<String, Object> updatedValues);

  /* for similarity */
  public abstract void initFeatureType();

  public abstract void initFeatureWeight();

  public abstract void spatialSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder);

  public abstract void temporalSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder);

  public abstract void categoricalVariablesSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder);

  public abstract void ordinalVariablesSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder);
}
