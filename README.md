# SecTweet
SecTweet is a Flink job that streams Twitter posts.  Relevent text containing shas, file paths, etc, are aggregated 
and trends are determined based on the frequency of these terms over time.  
  
For the Flink workshop, please follow the steps [here](https://github.com/Cisco-AMP/sectweet/wiki/Workshop) instead.

## Requirements
Java 8

## Services Setup
Services: Flink, Elasticsearch (Optional)
  
To start all containers:  
``` runDockerContainers.sh ```  
To start a specific container:  
``` docker-compose up <flink-jm | flink-tm | elasticsearch> ```

## Build
To package your job for submission to Flink, use: 'gradle shadowJar'. Afterwards, you'll find the
jar to use in the 'build/libs' folder.

To run and test your application with an embedded instance of Flink use: 'gradle run'

