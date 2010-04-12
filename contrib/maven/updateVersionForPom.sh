#!/bin/bash

#silly script that updates the the version inside the pom.xml from the ../../build.properties

OLD_VERSION=`sed '/^\#/d' mavenVoldemort.properties | grep 'mavenVoldemortVersion'  | tail -n 1 | sed 's/^.*=//;s/^[[:space:]]*//;s/[[:space:]]*$//'`
NEW_VERSION=`sed '/^\#/d' ../../build.properties | grep 'curr.release'  | tail -n 1 | sed 's/^.*=//;s/^[[:space:]]*//;s/[[:space:]]*$//'`


echo "Updating $OLD_VERSION to $NEW_VERSION inside pom.xml" 

echo "mavenVoldemortVersion=$NEW_VERSION" > mavenVoldemort.properties
perl -pi -e s/$OLD_VERSION/$NEW_VERSION/g pom.xml
