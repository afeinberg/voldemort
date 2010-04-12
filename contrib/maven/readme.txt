2010-04-11
Tom Chen
This is an example voldemort client maven pom.xml file. 
You can use this to kickstart or integrate into an existing java project.
Or just use it to figure out what the client dependencies are :)

To Start:
1. Make sure you have a build version of voldemort -- use ant to build it

2. Make sure you have maven installed :P 

3. Double check the versions of the pom.xml to ensure that it matches voldemort.

See the version of the project and check the system paths. When I created the 
pom.xml, voldemort was on version 0.80.1 

I added the minimal jars, but I suspect that I am missing the apache avro one. 

4. To build the project

cd contrib/maven
mvn clean install -Dmaven.test.skip=true

5. To run the client example. See pom.xml to see how the one jar is used. 

java -jar target/voldemort-client-0.81.one-jar.jar