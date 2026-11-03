# Sourced by Hadoop scripts (copied into build/hadoop/conf by cluster.sh).
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
# Allow running the daemons as root inside a dev container.
export HDFS_NAMENODE_USER=root HDFS_DATANODE_USER=root HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root YARN_NODEMANAGER_USER=root
# Hadoop 3.3 on Java 17 needs a few module opens (Guice/reflection in the YARN web apps).
export HADOOP_OPTS="${HADOOP_OPTS:-} --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED"
