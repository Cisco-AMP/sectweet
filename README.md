![](https://github.com/Cisco-AMP/sectweet/workflows/Java%20CI/badge.svg)

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
To build the project:  
```./gradlew clean build```  

## Running SecTweet
**Arguments**:  
Live twitter mode:  
```--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>```
  
File mode:  
```--file-source <twitter data file>```  
Note: there are test files available under ./data.  Simply unzip them to use in sectweet.                        
                                   
  
To run the project with embedded Flink:  
```./gradlew run --args='<flink args here>'```

## Packaging
To package your job for submission to Flink, use: 
```gradle shadowJar```  
Afterwards, you'll find the
jar to use in the 'build/libs' folder.

