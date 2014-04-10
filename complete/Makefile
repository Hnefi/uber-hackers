ZKPORT=ugNNN.eecg:NNNN
MYPORT=NNNN
DICT=dictFile.txt
PARTID=-1
TOTALPARTS=-1

build:
	javac -source 1.6 -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar FileServer.java DictionaryDB.java ZkConnector.java ZkPacket.java JobTracker.java ClientRequest.java DictionaryRequestPacket.java TrackerResponse.java Worker.java

runFS:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer $(DICT) $(ZKPORT) $(MYPORT) 

runJT:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker $(ZKPORT) $(MYPORT)

runWkr:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker $(ZKPORT) $(MYPORT)
clean:
	rm -f *.class
