package eu.wiszniewski.client;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import lombok.Data;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Optional;

public class ElasticSearchClient implements AutoCloseable{
    static final String SCROLL_ID = "scroll_id";
    private RestClient restClient;
    private ScrollRequest scrollRequest = new ScrollRequest();
    Gson gson = new Gson();

    @Data
    private class ScrollRequest {
        String scrollId;
        String scrollTime;
        JsonObject jsonBody;
        String url;
    }

    private static final Logger logger = LogManager.getLogger(ElasticSearchClient.class);

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

    public void initializeScroll(String index, String params, String body) {
        if (!params.startsWith("scroll=")) {
            logger.error("Invalid scroll initialization request: {}, {}, {}", index, params, body);
            throw new IllegalArgumentException("Invalid scroll initialization request, requires scroll=<time> parameter.");
        }
        try {
            scrollRequest.setJsonBody(gson.fromJson(body, JsonObject.class));
        } catch (JsonSyntaxException ex) {
            logger.error("Invalid body (not a valid json): {}", body);
            throw new IllegalArgumentException("Invalid body, should be json: " + body);
        }
        if (!scrollRequest.getJsonBody().has("size")) {
            logger.error("Invalid body, required size parameter not found: {}", body);
            throw new IllegalArgumentException("Invalid body, required size parameter: " + body);
        }
        scrollRequest.setUrl(index + "/_search?" + params);
        scrollRequest.setScrollId("");
        scrollRequest.setScrollTime(body.strip().split("=")[1]);
    }

    public Optional<Response> getScroll() {
        Optional<Response> response;
        if (scrollRequest.getScrollId().isBlank()) {
            response = sendRequest("POST", scrollRequest.getUrl(), scrollRequest.getJsonBody());
        } else {
            JsonObject scrollBody = new JsonObject();
            scrollBody.addProperty("scroll", scrollRequest.getScrollTime());
            scrollBody.addProperty(SCROLL_ID, scrollRequest.getScrollId());
            response = sendRequest("POST", "/_search/scroll", scrollBody);
        }
        if (response.isPresent())
            try {
                String responseContent = EntityUtils.toString(response.get().getEntity());
                JsonObject responseJson = gson.fromJson(responseContent, JsonObject.class);
                scrollRequest.setScrollId(responseJson.getAsJsonObject(SCROLL_ID).getAsString());
            } catch (IOException ex) {
                logger.error("Coulnd't unpack response body. Response: {}", response);
                return Optional.empty();
            }
        return response;
    }

    public Optional<Response> clearScroll() {
        Optional<Response> response;
        if (scrollRequest.getScrollId().isBlank()) {
            logger.info("No scroll request to clear found.");
            response = Optional.empty();
        } else {
            JsonObject scrollBody = new JsonObject();
            scrollBody.addProperty(SCROLL_ID, scrollRequest.getScrollId());
            response = sendRequest("DELETE", "/_search/scroll", scrollBody);
            scrollRequest = new ScrollRequest();
        }
        return response;
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
            logger.error("Invalid body (not a valid json): {}", body);
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

