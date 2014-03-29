build:
	javac -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar ZkConnector.java ZkPacket.java A.java B.java
runA:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. A ug201.eecg:8095
runB:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. B ug201.eecg:8095
clean:
	rm -f *.class
