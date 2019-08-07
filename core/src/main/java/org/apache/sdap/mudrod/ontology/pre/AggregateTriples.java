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
package org.apache.sdap.mudrod.ontology.pre;

import org.apache.commons.io.FilenameUtils;
import org.apache.sdap.mudrod.discoveryengine.DiscoveryStepAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.filter.ElementFilter;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Supports ability to extract triples (subclassOf, equivalent class) from OWL file
 */
public class AggregateTriples extends DiscoveryStepAbstract {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AggregateTriples.class);

  public AggregateTriples(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Method of executing triple aggregation
   */
  @Override
  public Object execute() {
    File file = new File(this.props.getProperty(MudrodConstants.ONTOLOGY_PATH));
    if (file.exists()) {
      file.delete();
    }
    try {
      file.createNewFile();
    } catch (IOException e2) {
      LOG.error("File creation failed: ", e2);
    }

    try(OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(file.getAbsoluteFile()), StandardCharsets.UTF_8)) {
      OSW = osw;
      File[] files = new File(this.props.getProperty(MudrodConstants.ONTOLOGY_INPUT_PATH)).listFiles();
      for (File fileIn : files) {
        String ext = FilenameUtils.getExtension(fileIn.getAbsolutePath());
        if ("owl".equals(ext)) {
          try {
            loadXml(fileIn.getAbsolutePath());
            getAllClass();
          } catch (JDOMException | IOException e1) {
            LOG.error("Couldn't load Owl file into memory: ", e1);
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Couldn't open file!", e);
    }

    return null;
  }

  private Document document;
  private Element rootNode = null;
  private final static String owl_namespace = "http://www.w3.org/2002/07/owl#";
  private final static String rdf_namespace = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
  private final static String rdfs_namespace = "http://www.w3.org/2000/01/rdf-schema#";

  OutputStreamWriter OSW = null;

  /**
   * Load OWL file into memory
   *
   * @param filePathName local path of OWL file
   * @throws JDOMException JDOMException
   * @throws IOException   IOException
   */
  private void loadXml(String filePathName) throws JDOMException, IOException {
    SAXBuilder saxBuilder = new SAXBuilder();
    File file = new File(filePathName);

    document = saxBuilder.build(file);
    rootNode = document.getRootElement();
  }

  /**
   * Method of going through OWL structure
   */
  public void loopxml() {
    Iterator<?> processDescendants = rootNode.getDescendants(new ElementFilter());
    String text = "";

    while (processDescendants.hasNext()) {
      Element e = (Element) processDescendants.next();
      String currentName = e.getName();
      text = e.getTextTrim();
      if ("".equals(text)) {
        LOG.info(currentName);
      } else {
        LOG.info("{} : {}", currentName, text);
      }
    }
  }

  /**
   * Method of identifying a specific child given a element name
   *
   * @param str element name
   * @param ele parent element
   * @return the element of child
   */
  private Element findChild(String str, Element ele) {
    Iterator<?> processDescendants = ele.getDescendants(new ElementFilter());
    String name = "";
    Element result = null;

    while (processDescendants.hasNext()) {
      Element e = (Element) processDescendants.next();
      name = e.getName();
      if (name.equals(str)) {
        result = e;
        return result;
      }
    }
    return result;

  }

  /**
   * Method of extract triples (subclassOf, equivalent class) from OWL file
   *
   * @throws IOException IOException
   */
  private void getAllClass() throws IOException {
    List<?> classElements = rootNode.getChildren("Class", Namespace.getNamespace("owl", owl_namespace));

    for (Object classElement1 : classElements) {
      Element classElement = (Element) classElement1;
      String className = classElement.getAttributeValue("about", Namespace.getNamespace("rdf", rdf_namespace));

      if (className == null) {
        className = classElement.getAttributeValue("ID", Namespace.getNamespace("rdf", rdf_namespace));
      }

      List<?> subclassElements = classElement.getChildren("subClassOf", Namespace.getNamespace("rdfs", rdfs_namespace));
      for (Object subclassElement1 : subclassElements) {
        Element subclassElement = (Element) subclassElement1;
        String subclassName = subclassElement.getAttributeValue("resource", Namespace.getNamespace("rdf", rdf_namespace));
        if (subclassName == null) {
          Element allValuesFromEle = findChild("allValuesFrom", subclassElement);
          if (allValuesFromEle != null) {
            subclassName = allValuesFromEle.getAttributeValue("resource", Namespace.getNamespace("rdf", rdf_namespace));
            OSW.write(cutString(className) + ",SubClassOf," + cutString(subclassName) + "\n");
          }
        } else {
          OSW.write(cutString(className) + ",SubClassOf," + cutString(subclassName) + "\n");
        }

      }

      List equalClassElements = classElement.getChildren("equivalentClass", Namespace.getNamespace("owl", owl_namespace));
      for (Object equalClassElement1 : equalClassElements) {
        Element equalClassElement = (Element) equalClassElement1;
        String equalClassElementName = equalClassElement.getAttributeValue("resource", Namespace.getNamespace("rdf", rdf_namespace));

        if (equalClassElementName != null) {
          OSW.write(cutString(className) + ",equivalentClass," + cutString(equalClassElementName) + "\n");
        }
      }

    }
  }

  /**
   * Method of cleaning up a string
   *
   * @param str String needed to be processed
   * @return the processed string
   */
  private String cutString(String str) {
    str = str.substring(str.indexOf("#") + 1);
    String[] strArray = str.split("(?=[A-Z])");
    str = Arrays.toString(strArray);
    return str.substring(1, str.length() - 1).replace(",", "");
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
