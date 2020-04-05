# es-java-client
## Why this project?
Client code for older version of ElasticSearch (5.6). It uses ES low level client to communicate with ES server.

As i have frequently the need to communicate with 5.6 version of ES server, I keep reusing the same code to communicate
 and send my rest api requests. So this is a library which will simplify my calls for it. It's not planned as an 
 implementation of regular high-level ES client code, rather an intermediary - to simplify sending REST requests, 
 scrolling through responses and handling response codes. 
 
## How to use it?
1) Initialize client 
```$xslt
ElasticSearchClient client = new ElasticSearchClient(<address>, <port>);
```
2) Once you're done close it:
```$xslt
client.close();
```

As it's implementing AutoClosable, you can use try with resource construction:
```$xslt
try (ElasticSearchClient client = new ElasticSearchClient(<address>, <port>) {
    ... your code ...
} catch (IOException ex) {
    ... your code ...
}
```


