**Analyze the problem**

*1. Merge files*

In the current process all the data is read in memory and then result is written back to the disk.
Since the size of the input data is growing, the fact that we are loading all the data to the memory could be problematic.
Also, since the result of merge is going to grow exponentially compared to input data, searching or applying metrics on this data could be problematic.


*2.  Calculate word frequency*

The problem here again is the final merged data is growing more rapidly compared to input data, and it's highly possible that there could be
problems while trying to fit all data in memory. Also, we could be calculating this metric while we are merging files. This way, we can avoid
reprocessing the data.


**Define the solution**

The solution proposed here is to monitor all the data in the 'input' directory. If we add a new file in the input directory, then
application will process immediately the new file, merge it with the previous processed result in streaming fashion and calculate count metrics.


More on detail on the solution;

*1. Merge files*

Application is going to partition data by the first letter of elements in the input data. All files sharing the same letter will be in the same output
partition. In the output directory we will have as many subdirectories as the unique first letters in all elements in all files.
This gives us the ability to apply custom metrics or ability to search in a more efficient way afterwards.



*2.  Calculate word frequency*

Merges all files in the input directory (we could add also a new files into directory while this application is running)
And writes them into output directory with the total number of occurrence of that element in all files.  
This count value is updated everytime we add new file to the directory.
In the output directory we will one file containing all the summary of key and count pair.


**Describe the pros and cons of the solution**

*Pros:*

1. This is an optimized stateful streaming job. This application does not load all the data in memory and uses distributed computing resources.

2. This is a reliable application, in case of a failure, the application will restart from where it was left off.

3. New files are merged and metrics are calculated automaticaly, no further action is needed.

4. In summary, we are solving the problems that we have mentioned in analyze the problem section.

*Cons:*

1. No rollback available for the moment. If we were given a corrupted input, then the result will be wrong as well.  
We could introduce maybe versioning of the recent data, or have more granularity on the partition data (such as add date to partitionCol)  
So that we can delete all files corresponding to the corrupted input.

2. This solution assumes that data is uniformly distributed by the first letter of elements in input data. If not, we could have some hot partition problems.

3. For the moment, this application is merging all files in the given input directory and calculating one count metric.
But there could be a need to merge only specific files (excluded some) and calculate the metrics for those specific files.


**Write code that is reliable, testable and maintainable**

This code was written to be reliable, maintainable, testable and scalable.  
It has a lot of room for development, automating testing, logging, monitoring etc.


**Write a unit test case and how to run it ?**
Since I was given a limited time, I couldn't write all the unit tests and the current test could be improved.

The unit test was written for 'count metric'. The idea behind of it is, we provide a small file that we know the number of counts per name.
And then we simply compare the result with our expected values.

This could be improved by creating multiple test files and parameterizing expected result.

And could be ran by;

*mvn test*


**Prerequisites**

You need to have maven

and docker (if you want to launch the application on a docker cluster)

installed

**Installation**


**Launch on a spark cluster with docker:**


1. git clone https://github.com/mertozer94/MergeFiles.git

2. Create jar at the root of the project
cd /path/toYourProject
mvn clean package

3. Modify docker-compose.yml

Change all of the occurences of '/home/mert/IdeaProjects/VeeavaMergeFiles' to {/path/toYourProject}

4. Launch spark cluster
docker-compose up -d

go to http://localhost:8080/

and save spark url we are going to use to submit our job
url will look like the following: spark://bbbb2660e3f6:707

5. Connect to the docker container

docker-compose exec spark bash

6. Submit the job

./bin/spark-submit --class com.merge.Main \  
--master {sparUrlFromStep4} \  
--deploy-mode client \  
{/path/toYourProject}/target/VeevaMergeFiles-1.0-jar-with-dependencies.jar \  
{sparUrlFromStep4} \  
{/path/toYourProject}/

The '/' at the end is important.

7.  Add data and verify results

put data in input directory and verify the files created in the output directory


Don't forget to stop cluster with
docker stop veeavamergefiles_spark_1 veeavamergefiles_spark-worker-1_1 veeavamergefiles_spark-worker-2_1

**Launch it within your IDE locally:**

1. Open the project in your favorite IDE

2. mvn clean package

3. run the Main.scala class with giving two parameters spark url and path to this project

4. put data in input directory and verify the files created in the output directory

