#!/bin/bash

#get this directory's path
this_path=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

echo "Executing words_tweeted"

javac $this_path/src/words_tweeted.java
cd $this_path/src
java -cp . words_tweeted "$this_path"

echo "done"

echo "Executing median_unique"

javac $this_path/src/median_unique.java
cd $this_path/src
java -cp . median_unique "$this_path"

echo "done"
