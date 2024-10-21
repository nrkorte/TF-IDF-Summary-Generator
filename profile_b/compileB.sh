hadoop com.sun.tools.javac.Main ProfileB.java
rm classes/*
mv *.class classes
rm ProfileB.jar
jar cf ProfileB.jar classes/
hadoop fs -rm -r /PA2/profile*