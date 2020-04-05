package eu.wiszniewski.client;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Optional;

public class ElasticSearchClient implements AutoCloseable{
    private RestClient restClient;
    private static final Logger logger = LogManager.getLogger(ElasticSearchClient.class);
    Gson gson = new Gson();

    public ElasticSearchClient(String address, int port) {
        restClient = RestClient.builder(new HttpHost(address, port)).build();
    }

    public ElasticSearchClient(String address) {
        this(address, 9200);
    }

    public void close() throws IOException {
        try {
            restClient.close();
        } catch (IOException ex) {
            logger.error("Connection was not closed properly for restClient: {}", restClient);
            logger.error(ex.getMessage());
            throw ex;
        }
    }

    public Optional<Response> getById(String index, int id, String params) {
        return requestById("GET", index, id, params);
    }

    public Optional<Response> existsById(String index, int id, String params) {
        return requestById("HEAD", index, id, params);
    }

    public Optional<Response> deleteById(String index, int id, String params) {
        return requestById("DELETE", index, id, params);
    }

    public Optional<Response> getQuery(String index, String params, String body) {
        return queryRequest("GET", index, params, body);
    }

    public String initialScrollRequest(int pageSize) {
        //TODO: body
        return null;
    }

    private Optional<Response> requestById(String method, String index, int id, String params) {
        String request = index + "/_doc/" + id + "?" + params;
        JsonObject body = new JsonObject();
        return sendRequest(method, request, body);
    }

    private Optional<Response> queryRequest(String method, String index, String params, String body) {
        JsonObject jsonBody;
        try {
            jsonBody = gson.fromJson(body, JsonObject.class);
        } catch (JsonSyntaxException ex) {
            throw new IllegalArgumentException("Invalid body, should be json: " + body);
        }
        String request = index + "/_search?" + params;
        return sendRequest(method, request, jsonBody);
    }

    private Optional<Response> sendRequest(String method, String url, JsonObject body) {
        Request request = new Request(method, url);
        if (body.size() > 0)
            request.setEntity(new NStringEntity(body.toString(), ContentType.APPLICATION_JSON));
        logger.debug("Prepared request for ES: {}", request);

        Optional<Response> response = Optional.empty();

        try {
            response = Optional.of(restClient.performRequest(request));
        } catch (ResponseException ex) {
            response = Optional.of(ex.getResponse());
        } catch (IOException ex) {
            logger.error("Communication problem: {}", ex.getMessage());
        }

        return response;
    }

}