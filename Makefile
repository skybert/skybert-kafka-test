# Makefile for the Kafka CLI app
#
# Author: torstein@skybert.net

all: format run-faster-reads

# Compile app
compile:
	mvn clean
	mvn install

# Run app
run:
	mvn exec:java -Dexec.mainClass="net.skybert.kafka.KafkaApp"

run-faster-reads:
	mvn exec:java -Dexec.mainClass="net.skybert.kafka.KafkaAppFasterReads"


# Format the source code
format:
	google-java-format --replace src/main/java/net/skybert/kafka/*.java

# Upgrade 3rd party dependencies except alpha, beta, RC and M
# versions.
upgrade:
	mvn versions:use-latest-releases \
	  -DallowMajorUpdates=true \
	  -Dexcludes='*:*:*:*alpha*,*:*:*:*beta*,*:*:*:*RC*,*:*:*:*M*'

