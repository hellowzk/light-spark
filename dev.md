install 到项目组 maven 仓库
```shell script
mvn clean scala:compile compile install deploy -Dmaven.test.skip=true
```