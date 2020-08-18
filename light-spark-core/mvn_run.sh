mvn clean scala:compile compile install -Pspark2.4 -Dmaven.test.skip=true
mvn scala:doc
cp -r /d/code/light-spark/light-spark-core/target/site /d/code/light-spark/light-spark-core/target/apidocs
mvn install -Pspark2.4 -Dmaven.test.skip=true
mvn deploy -Pspark2.4 -Dmaven.test.skip=true