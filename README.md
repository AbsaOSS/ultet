# ultet

### Building and testing
To build and test the package locally, run:
```
sbt clean test
```

### How to generate Code coverage report
```sbt
sbt ++{scala_version} jacoco
```
Code coverage will be generated on path:
```
{project-root}/target/scala-{scala_version}/jacoco/report/html
```