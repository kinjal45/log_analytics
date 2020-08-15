package Utils;


import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.spark.broadcast.Broadcast;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ElasticsearchDao {

    private final RestHighLevelClient esClient;
    private final BulkProcessor bulkProcessor;

    public ElasticsearchDao(RestHighLevelClient client, Broadcast<JSONObject> conf) {
        this.esClient = client;
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory
                        .HeapBufferedResponseConsumerFactory(1024 * 1024 * 1024));

        this.bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) ->

                        esClient.bulkAsync(request, builder.build(), bulkListener),
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        //System.out.println("going to execute bulk of {} requests");
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        System.out.println("bulk executed {} success - " +response.status());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        System.out.println("error while executing bulk - " + failure);
                    }
                })
                .setBulkActions(Integer.parseInt(conf.value().getString("bulkAction")))
                .setBulkSize(new ByteSizeValue(Integer.parseInt(conf.value().getString("byteSize")), ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueMillis(Integer.parseInt(conf.value().getString("flushInterval"))))
                .setConcurrentRequests(Integer.parseInt(conf.value().getString("concurrentRequest")))
                .setBackoffPolicy(
                        BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(500), 3))
                .build();
    }

    public void save(Map<String, Object> map, String index) throws JsonProcessingException {
        bulkProcessor.add(new IndexRequest(index).source(map));

    }

    public void flush(){
        try{
            bulkProcessor.flush();
            bulkProcessor.awaitClose(60,TimeUnit.SECONDS);
        }catch (Exception e){

        }
    }


    public void delete(String id) {
        bulkProcessor.add(new DeleteRequest("person", id));
    }

    public SearchResponse search(QueryBuilder query, Integer from, Integer size) throws IOException {
        System.out.println("elasticsearch query: {}" + query.toString());
        SearchResponse response = esClient.search(new SearchRequest("person")
                .source(new SearchSourceBuilder()
                        .query(query)
                        .from(from)
                        .size(size)
                        .trackTotalHits(true)
                ), RequestOptions.DEFAULT);

        System.out.println("elasticsearch response: {} hits" + response.getHits().getTotalHits());

        return response;
    }
}
