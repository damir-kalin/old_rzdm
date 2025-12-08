# Обязательное условие для корресктной компиляции необходима, чтобы в рабочей среде были установлены следующие компоненты:

damir@sky:~$ mvn --version
Apache Maven 3.8.7
Maven home: /usr/share/maven
Java version: 17.0.17, vendor: Ubuntu, runtime: /usr/lib/jvm/java-17-openjdk-amd64
Default locale: ru_RU, platform encoding: UTF-8
OS name: "linux", version: "6.14.0-36-generic", arch: "amd64", family: "unix"

damir@sky:~$ java --version
openjdk 17.0.17 2025-10-21
OpenJDK Runtime Environment (build 17.0.17+10-Ubuntu-124.04)
OpenJDK 64-Bit Server VM (build 17.0.17+10-Ubuntu-124.04, mixed mode, sharing)

# Kоманда для компиляции задач Flink в jar
mvn clean package -DskipTests
