# SecTweet
SecTweet is a Flink job that streams Twitter posts.  Relevent text containing shas, file paths, etc, are aggregated 
and trends are determined based on the frequency of these terms over time.

## Setup
Services required: Flink, Elasticsearch   
  
To start all containers:  
``` runDockerContainers.sh ```  
To start a specific container:  
``` docker-compose up <flink-jm | flink-tm | elasticsearch> ```

## Build
To build the Flink job:   
``` gradle shadowJar ```
