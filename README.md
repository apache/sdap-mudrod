# MUDROD
## Mining and Utilizing Dataset Relevancy from Oceanographic Datasets to Improve Data Discovery and Access

[![license](https://img.shields.io/github/license/apache/incubator-sdap-mudrod.svg?maxAge=2592000?style=plastic)](http://www.apache.org/licenses/LICENSE-2.0)

[MUDROD](https://esto.nasa.gov/forum/estf2015/presentations/Yang_S8P1_ESTF2015.pdf) 
is a semantic discovery and search project part of [Apache SDAP](https://sdap.apache.org).

# Software requirements: 
 * Java 8
 * Git
 * Apache Maven 3.X
 * Elasticsearch v5.X
 * Kibana v4
 * Apache Spark v2.0.0
 * Apache Tomcat 7.X

# Installation

## Docker Container
We strongly advise all users to save time and effort by consulting the [Dockerfile documentation](https://github.com/mudrod/mudrod/tree/master/docker)
for guidance on how to quickly use Docker to deploy Mudrod.

## From source
1. Ensure you have Elasticsearch running locally and that the configuration in [config.xml](https://github.com/apache/incubator-sdap-mudrod/blob/master/core/src/main/resources/config.xml) reflects your ES cluster.
2. Update the `svmSgdModel` configuration option in [config.xml](https://github.com/apache/incubator-sdap-mudrod/blob/master/core/src/main/resources/config.xml). There is a line in config.xml that looks like 
    ```
    <para name="svmSgdModel">file://YOUNEEDTOCHANGETHIS</para>
    ```
    It needs to be changed to an absolute filepath on your system. For example:
    ```
    <para name="svmSgdModel">file:///Users/user/githubprojects/mudrod/core/src/main/resources/javaSVMWithSGDModel</para>
    ```
3. (Optional) Depending on your computer's configuration you might run into an error when starting the application: `“Service 'sparkDriver' could not bind on port 0”`. The easiest [fix](http://stackoverflow.com/q/29906686/953327) is to export the environment variable `SPARK_LOCAL_IP=127.0.0.1 ` and then start the service.

```
$ git clone https://github.com/mudrod/mudrod.git
$ cd mudrod
$ mvn clean install
$ cd service
$ mvn tomcat7:run
```
You will now be able to access the Mudrod Web Application at [http://localhost:8080/mudrod-service](http://localhost:8080/mudrod-service). **N.B.** The service should not be run this way in production.

# Documentation

In another window...
```
$ cd mudrod
$ ./core/target/appassembler/bin/mudrod-engine -h
usage: MudrodEngine: 'logDir' argument is mandatory. User must also
       provide an ingest method. [-a] [-esHost <host_name>] [-esPort
       <port_num>] [-esTCPPort <port_num>] [-f] [-h] [-l] -logDir
       </path/to/log/directory> [-p] [-s] [-v]
 -a,--addSimFromMetadataAndOnto                          begin adding
                                                         metadata and
                                                         ontology results
 -esHost,--elasticSearchHost <host_name>                 elasticsearch
                                                         cluster unicast
                                                         host
 -esPort,--elasticSearchHTTPPort <port_num>              elasticsearch
                                                         HTTP/REST port
 -esTCPPort,--elasticSearchTransportTCPPort <port_num>   elasticsearch
                                                         transport TCP
                                                         port
 -f,--fullIngest                                         begin full ingest
                                                         Mudrod workflow
 -h,--help                                               show this help
                                                         message
 -l,--logIngest                                          begin log ingest
                                                         without any
                                                         processing only
 -logDir,--logDirectory </path/to/log/directory>         the log directory
                                                         to be processed
                                                         by Mudrod
 -p,--processingWithPreResults                           begin processing
                                                         with
                                                         preprocessing
                                                         results
 -s,--sessionReconstruction                              begin session
                                                         reconstruction
 -v,--vocabSimFromLog                                    begin similarity
                                                         calulation from
                                                         web log Mudrod
                                                         workflow
```

## Deploying to Apache Tomcat (or any other Servlet container)
Once you have built the codebase as above, merely copy the genrated .war artifact to the servlet deployment directory. In Tomcat (for example), this would look as follows
```
$ cp mudrod/service/target/mudrod-service-${version}-SNAPSHOT.war $CATALINA_HOME/webapps/
```
Once Tomcat hot deploys the .war artifact, you will be able to browse to the running application similar to what is shown above [http://localhost:8080/mudrod-service](http://localhost:8080/mudrod-service)

## Mudrod Wiki

https://cwiki.apache.org/confluence/display/SDAP/Home

## Java API Documentation

```
$ mvn javadoc:aggregate
$ open target/site/apidocs/index.html
```

## REST API Documentation

```
$ mvn clean install
$ open service/target/miredot/index.html
```
The REST API documentation can also be seen at [https://sdap.apache.org/miredot](https://sdap.apache.org/miredot).

# License
This source code is licensed under the [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0), a
copy of which is shipped with this project. 
