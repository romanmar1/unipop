package org.unipop.elastic.helpers;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.junit.Before;
import org.junit.Test;
import org.unipop.elastic.controller.schema.helpers.AggregationBuilder;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/16/2015.
 */
public class AggregationBuilderTests {
    @Before
    public void setup() throws Exception {
        Node node = ElasticClientFactory.createNode("Test", false, 0);
        client = node.client();
    }

    @Test
    public void FiltersSimpleTest() {
        AggregationBuilder aggregationBuilder = new AggregationBuilder();
        aggregationBuilder.filters("myFilters")
                .filter("myFilter1", new QueryBuilder().query().filtered().filter().bool().must().term("prop1", "value1").term("prop2", "value2")).seek("myFilters")
                .filter("myFilter2", new QueryBuilder().query().filtered().filter().bool().should().terms("prop3", Arrays.asList("value3", "value4", "value5")));

        SearchRequestBuilder searchRequest = client.prepareSearch();
        StreamSupport.stream(aggregationBuilder.getAggregations().spliterator(), false).collect(Collectors.toList()).forEach(searchRequest::addAggregation);

        String searchREquestString = searchRequest.toString();
        int x = 5;
    }

    @Test
    public void FiltersAdvancedTest() {
        AggregationBuilder aggregationBuilder = new AggregationBuilder();
        aggregationBuilder.filters("myFilters")
                .filter("myFilter1", new QueryBuilder().query().filtered().filter().bool().must().term("prop1", "value1").term("prop2", "value2")).seek("myFilters")
                .filter("myFilter2", new QueryBuilder().query().filtered().filter().bool().should().terms("prop3", Arrays.asList("value3", "value4", "value5"))).seek("myFilters")
                .filters("myInnerFilters")
                    .filter("myInnerFilter1", new QueryBuilder().query().filtered().filter().bool().mustNot().exists("prop4")).seek("myInnerFilters")
                    .filter("myInnerFilter2", new QueryBuilder().query().filtered().filter().bool().must().ids(Arrays.asList("1", "2", "3"), "myType"));

        SearchRequestBuilder searchRequest = client.prepareSearch();
        StreamSupport.stream(aggregationBuilder.getAggregations().spliterator(), false).collect(Collectors.toList()).forEach(searchRequest::addAggregation);

        String searchREquestString = searchRequest.toString();
        int x = 5;
    }

    //region Fields
    private Client client;
    //endregion
}
