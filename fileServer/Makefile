ZKPORT=ugNNN.eecg:NNNN
MYPORT=NNNN
DICT=dictFile.txt
PARTID=-1
TOTALPARTS=-1

build:
	javac -source 1.6 -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar FileServer.java DictionaryDB.java
run:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer $(DICT) $(PARTID) $(TOTALPARTS) $(ZKPORT) $(MYPORT) 
clean:
	rm -f *.class
