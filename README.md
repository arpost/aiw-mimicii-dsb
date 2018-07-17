# Protempa MIMIC II data source backend
[Department of Biomedical Informatics](http://bmi.emory.edu), [Emory University](http://www.emory.edu), Atlanta GA

## What is it?
Protempa data source backend for accessing the MIMIC II clinical database version 2v26.

Latest release: [![Latest release](https://maven-badges.herokuapp.com/maven-central/org.eurekaclinical/aiw-mimicii-dsb/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.eurekaclinical/aiw-mimicii-dsb)

## Version 3.0
Updated Protempa version requirements.

## Version 2.0
Updated Protempa version requirements.

## Version 1.0
Initial release.

# Build requirements
* [Oracle Java JDK 8](http://www.oracle.com/technetwork/java/javase/overview/index.html)
* [Maven 3.2.5 or greater](https://maven.apache.org)

## Runtime requirements
* [Oracle Java JRE 8](http://www.oracle.com/technetwork/java/javase/overview/index.html)
* [A copy of the MIMIC II clinical database version 2v26](https://physionet.org/mimic2/)

## Building it
The project uses the maven build tool. Typically, you build it by invoking `mvn clean install` at the command line. For simple file changes, not additions or deletions, you can usually use `mvn install`. See https://github.com/eurekaclinical/dev-wiki/wiki/Building-Eureka!-Clinical-projects for more details.

## Maven dependency
```
<dependency>
    <groupId>org.eurekaclinical</groupId>
    <artifactId>aiw-mimicii-dsb</artifactId>
    <version>version</version>
</dependency>
```

## Installation
Put the `aiw-mimicii-dsb` jarfile and its dependencies in the classpath, and Protempa will automatically register the data source backend.

## Using it

### Backend configuration

#### `edu.emory.cci.aiw.i2b2etl.dsb.I2b2DataSourceBackend`
* `databaseAPI`: `DRIVERMANAGER` or `DATASOURCE` depending on whether the `databaseId` property contains a JDBC URL or a JNDI URL, respectively.
* `databaseId`: a JDBC URL or a JNDI URL for connecting to the i2b2 data schema.
* `username`: for JDBC URLs, a database username.
* `password`: for JDBC URLs, a database password.
* `schemaName`: the name of the data schema to query. If specified, the schema name will be included in queries. If not specified, no schema name will be specified, and the schema that will be queried will be database dependent. Typically, the database will query the user's default schema.

A Protempa INI config file must contain the following section for configuring the MIMIC II data source backend:
```
[edu.emory.cci.aiw.dsb.mimicii.MIMIC2v26DataSourceBackend]
dataSourceBackendId=Unique identifier for this i2b2 repository
databaseId = JDBC URL for the data schema
username = username with privileges to read the data schema
password = data schema user's password
schemaName = name of the data schema

## Developer documentation
[Javadoc for latest development release](http://javadoc.io/doc/org.eurekaclinical/aiw-mimicii-dsb) [![Javadocs](http://javadoc.io/badge/org.eurekaclinical/aiw-mimicii-dsb.svg)](http://javadoc.io/doc/org.eurekaclinical/aiw-mimicii-dsb)
