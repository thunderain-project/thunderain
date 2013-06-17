#!/bin/bash

root=$(
  cd $(dirname $(readlink $0 || echo $0))
  /bin/pwd
)

shark_config=$root/conf/shark-env.sh

if [ -e $shark_config ]; then
  . $shark_config
fi

if [ "$HIVE_HOME" == "" ]; then
  echo "HIVE_HOME should be set at first"
  exit 1
fi

sbtjar=sbt-launch.jar

if [ ! -f $sbtjar ]; then
  echo 'downloading '$sbtjar 1>&2
  curl -O http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.11.3-2/$sbtjar
fi

java -ea                          \
  $JAVA_OPTS                      \
  -Djava.net.preferIPv4Stack=true \
  -XX:+AggressiveOpts             \
  -XX:+UseParNewGC                \
  -XX:+UseConcMarkSweepGC         \
  -XX:+CMSClassUnloadingEnabled   \
  -XX:MaxPermSize=1024m           \
  -Xss8M                          \
  -Xms512M                        \
  -Xmx1G                          \
  -server                         \
  -jar $sbtjar "$@"
