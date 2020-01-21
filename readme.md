
### more info:
https://apacheignite-fs.readme.io/docs/ignite-data-frame#section-reading-dataframes

### more examples here:
https://github.com/apache/ignite/tree/master/examples/src/main/spark/org/apache/ignite/examples/spark

more


### To setup:
intellij setup scala spark

install jdk first 
https://www.oracle.com/technetwork/java/javase/downloads/index.html

setup jdk(link below up to add jdk)
https://guides.codepath.com/android/setting-up-intellij-idea

use scala directly in intellij
https://www.scala-lang.org/download/


when openinig projects:
right click on project add framework support add maven

mvn clean package in terminial

on project add global library and scala 2.11

in plugins search for Azure and will find an Azure Toolkit for IntelliJ. Please install this plugin to your IntelliJ.
https://www.c-sharpcorner.com/article/working-with-spark-and-scala-in-intellij-idea-part-i/


this workinig version of scala and spark and ignte needs jdk 8


https://www.jetbrains.com/help/idea/convert-a-regular-project-into-a-maven-project.html


in the end I made a new azure spark project and set the scala spark versions  as the pom 
there II could use the jdk 8 (jave 1.8) while defining the project
then added iiginte to the pom
and it works. 
mvn clean package does not work as it uses the systemwide java 13 but 