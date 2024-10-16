# compile maven project
compile:
	mvn clean
	mvn install

# run maven project
run:
	mvn exec:java -Dexec.mainClass="net.skybert.KafkaApp" -Dexec.args="server etc/kafka-test.yaml"
