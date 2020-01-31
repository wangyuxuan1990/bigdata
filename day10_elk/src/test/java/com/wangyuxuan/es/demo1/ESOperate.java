package com.wangyuxuan.es.demo1;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangyuxuan
 * @date 2020/1/31 4:19 下午
 * @description Elasticsearch的JavaAPI操作
 */
public class ESOperate {
    private TransportClient client;

    /**
     * 初始化ES客户端
     *
     * @throws UnknownHostException
     */
    @BeforeEach
    public void initClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "myes").put("client.transport.sniff", "true").build();
        client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("node01"), 9300));
    }

    /**
     * 插入json格式的索引数据
     */
    @Test
    public void createIndex() {
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"travelying out Elasticsearch\"" +
                "}";
        IndexResponse indexResponse = client.prepareIndex("myindex1", "article", "1").setSource(json, XContentType.JSON).get();
    }

    /**
     * 插入map格式的索引数据
     */
    @Test
    public void index2() {
        HashMap<String, String> jsonMap = new HashMap<>();
        jsonMap.put("name", "zhangsan");
        jsonMap.put("sex", "1");
        jsonMap.put("age", "18");
        jsonMap.put("address", "bj");
        IndexResponse indexResponse = client.prepareIndex("myindex1", "article", "2").setSource(jsonMap).get();
    }

    /**
     * 通过XContentBuilder来实现索引的创建
     *
     * @throws IOException
     */
    @Test
    public void index3() throws IOException {
        IndexResponse indexResponse = client.prepareIndex("myindex1", "article", "3").setSource(new XContentFactory().jsonBuilder().startObject()
                .field("name", "lisi")
                .field("age", "18")
                .field("sex", "0")
                .field("address", "bj")
                .endObject())
                .get();
    }

    /**
     * 将java对象转换为json格式字符串进行创建索引
     */
    @Test
    public void objToIndex() {
        Person person = new Person();
        person.setAge(18);
        person.setId(20);
        person.setName("张三丰");
        person.setAddress("武当山");
        person.setEmail("zhangsanfeng@163.com");
        person.setPhone("18588888888");
        person.setSex(1);
        String json = JSONObject.toJSONString(person);
        System.out.println(json);
        IndexResponse indexResponse = client.prepareIndex("myindex1", "article", "4").setSource(json, XContentType.JSON).get();
    }

    /**
     * 批量创建索引
     *
     * @throws IOException
     */
    @Test
    public void index4() throws IOException {
        BulkRequestBuilder bulk = client.prepareBulk();
        bulk.add(client.prepareIndex("myindex1", "article", "5")
                .setSource(new XContentFactory().jsonBuilder()
                        .startObject()
                        .field("name", "wangwu")
                        .field("age", "18")
                        .field("sex", "0")
                        .field("address", "bj")
                        .endObject()));
        bulk.add(client.prepareIndex("news", "article", "6")
                .setSource(new XContentFactory().jsonBuilder()
                        .startObject()
                        .field("name", "zhaoliu")
                        .field("age", "18")
                        .field("sex", "0")
                        .field("address", "bj")
                        .endObject()));
        BulkResponse bulkResponse = bulk.get();
        System.out.println(bulkResponse);
    }

    /**
     * 初始化一批数据到索引库当中去准备做查询使用
     * 注意这里初始化的时候，需要给我们的数据设置分词属性
     *
     * @throws IOException
     */
    @Test
    public void createIndexBatch() throws IOException {
        // 创建映射
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("id").field("type", "integer").endObject()
                .startObject("name").field("type", "text").field("analyzer", "ik_max_word").endObject()
                .startObject("age").field("type", "integer").endObject()
                .startObject("sex").field("type", "text").field("analyzer", "ik_max_word").endObject()
                .startObject("address").field("type", "text").field("analyzer", "ik_max_word").endObject()
                .startObject("phone").field("type", "text").endObject()
                .startObject("email").field("type", "text").endObject()
                .startObject("say").field("type", "text").field("analyzer", "ik_max_word").endObject()
                .endObject()
                .endObject();
        PutMappingRequest putmap = Requests.putMappingRequest("indexsearch").type("mysearch").source(mapping);
        // 创建索引
        client.admin().indices().prepareCreate("indexsearch").execute().actionGet();
        // 为索引添加映射
        client.admin().indices().putMapping(putmap).actionGet();

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        Person lujunyi = new Person(2, "玉麒麟卢俊义", 28, 1, "水泊梁山", "17666666666", "lujunyi@163.com", "hello world今天天气还不错");
        Person wuyong = new Person(3, "智多星吴用", 45, 1, "水泊梁山", "17666666666", "wuyong@163.com", "行走四方，抱打不平");
        Person gongsunsheng = new Person(4, "入云龙公孙胜", 30, 1, "水泊梁山", "17666666666", "gongsunsheng@163.com", "走一个");
        Person guansheng = new Person(5, "大刀关胜", 42, 1, "水泊梁山", "17666666666", "wusong@163.com", "我的大刀已经饥渴难耐");
        Person linchong = new Person(6, "豹子头林冲", 18, 1, "水泊梁山", "17666666666", "linchong@163.com", "梁山好汉");
        Person qinming = new Person(7, "霹雳火秦明", 28, 1, "水泊梁山", "17666666666", "qinming@163.com", "不太了解");
        Person huyanzhuo = new Person(8, "双鞭呼延灼", 25, 1, "水泊梁山", "17666666666", "huyanzhuo@163.com", "不是很熟悉");
        Person huarong = new Person(9, "小李广花荣", 50, 1, "水泊梁山", "17666666666", "huarong@163.com", "打酱油的");
        Person chaijin = new Person(10, "小旋风柴进", 32, 1, "水泊梁山", "17666666666", "chaijin@163.com", "吓唬人的");
        Person zhisheng = new Person(13, "花和尚鲁智深", 15, 1, "水泊梁山", "17666666666", "luzhisheng@163.com", "倒拔杨垂柳");
        Person wusong = new Person(14, "行者武松", 28, 1, "水泊梁山", "17666666666", "wusong@163.com", "二营长。。。。。。");

        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "1")
                .setSource(JSONObject.toJSONString(lujunyi), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "2")
                .setSource(JSONObject.toJSONString(wuyong), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "3")
                .setSource(JSONObject.toJSONString(gongsunsheng), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "4")
                .setSource(JSONObject.toJSONString(guansheng), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "5")
                .setSource(JSONObject.toJSONString(linchong), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "6")
                .setSource(JSONObject.toJSONString(qinming), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "7")
                .setSource(JSONObject.toJSONString(huyanzhuo), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "8")
                .setSource(JSONObject.toJSONString(huarong), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "9")
                .setSource(JSONObject.toJSONString(chaijin), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "10")
                .setSource(JSONObject.toJSONString(zhisheng), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "11")
                .setSource(JSONObject.toJSONString(wusong), XContentType.JSON)
        );

        bulkRequestBuilder.get();
    }

    /**
     * 通过id来进行精确查询
     */
    @Test
    public void query1() {
        GetResponse documentFields = client.prepareGet("indexsearch", "mysearch", "11").get();
        String index = documentFields.getIndex();
        String type = documentFields.getType();
        String id = documentFields.getId();
        System.out.println(index);
        System.out.println(type);
        System.out.println(id);
        Map<String, Object> source = documentFields.getSource();
        for (String s : source.keySet()) {
            System.out.println(source.get(s));
        }
    }

    /**
     * 查询所有数据
     */
    @Test
    public void queryAll() {
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch").setQuery(new MatchAllQueryBuilder()).get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }

    /**
     * 查找年龄18到28的人,包含18和28
     */
    @Test
    public void rangeQuery() {
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch").setQuery(new RangeQueryBuilder("age").gt("17").lte("28")).get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }

    /**
     * 词条查询
     */
    @Test
    public void termQuery() {
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch").setQuery(new TermQueryBuilder("say", "熟悉")).get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }

    /**
     * fuzzyQuery表示英文单词的最大可纠正次数，最大可以自动纠正两次
     */
    @Test
    public void fuzzyQuery() {
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch").setQuery(QueryBuilders.fuzzyQuery("say", "helOL").fuzziness(Fuzziness.TWO)).get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }

    /**
     * 模糊匹配查询有两种匹配符，分别是" * " 以及 " ? "， 用" * "来匹配任何字符，包括空字符串。用" ? "来匹配任意的单个字符
     */
    @Test
    public void wildCardQuery() {
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch").setQuery(QueryBuilders.wildcardQuery("say", "hel*")).get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }

    /**
     * 多条件组合查询 boolQuery
     * 查询年龄是18到28范围内且性别是男性的，或者id范围在10到13范围内的
     */
    @Test
    public void boolQuery() {
        RangeQueryBuilder age = QueryBuilders.rangeQuery("age").gt("17").lt("29");
        TermQueryBuilder sex = QueryBuilders.termQuery("sex", "1");
        RangeQueryBuilder id = QueryBuilders.rangeQuery("id").gt("9").lt("14");
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch").setQuery(QueryBuilders.boolQuery().should(id)
                .should(QueryBuilders.boolQuery().must(age).must(sex))).get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }

    /**
     * 分页查询
     */
    @Test
    public void getPageIndex() {
        int pageSize = 5;
        int pageNum = 2;
        int startNum = (pageNum - 1) * pageSize;
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch").setQuery(QueryBuilders.matchAllQuery())
                .addSort("id", SortOrder.ASC)
                .setFrom(startNum)
                .setSize(pageSize)
                .get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }

    /**
     * 高亮查询
     */
    @Test
    public void highLight() {
        // 设置我们的查询高亮字段
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("indexsearch").setTypes("mysearch").setQuery(QueryBuilders.termQuery("say", "hello"));
        // 设置我们字段高亮的前缀与后缀
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("say").preTags("<font style='color:red'>").postTags("</font>");
        // 通过高亮来进行我们的数据查询
        SearchResponse searchResponse = searchRequestBuilder.highlighter(highlightBuilder).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询出来一共" + hits.totalHits + "条数据");
        for (SearchHit hit : hits) {
            // 打印没有高亮显示的数据
            System.out.println(hit.getSourceAsString());
            System.out.println("====================================");
            // 打印我们经过高亮显示之后的数据
            Text[] says = hit.getHighlightFields().get("say").getFragments();
            for (Text say : says) {
                System.out.println(say);
            }
        }
    }

    /**
     * 更新索引
     * 根据数据id来进行更新索引
     */
    @Test
    public void updateIndex() {
        Person guansheng = new Person(5, "宋江", 88, 0, "水泊梁山", "17666666666", "wusong@kkb.com", "及时雨宋江");
        client.prepareUpdate().setIndex("indexsearch").setType("mysearch").setId("5").setDoc(JSONObject.toJSONString(guansheng), XContentType.JSON).get();
    }

    /**
     * 按照id进行删除数据
     */
    @Test
    public void deleteById() {
        DeleteResponse deleteResponse = client.prepareDelete("indexsearch", "mysearch", "11").get();
    }

    /**
     * 按照条件进行删除
     */
    @Test
    public void deleteByQuery() {
        BulkByScrollResponse bulkByScrollResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.rangeQuery("id").gt("2").lt("4"))
                .source("indexsearch")
                .get();
        long deleted = bulkByScrollResponse.getDeleted();
        System.out.println(deleted);
    }

    /**
     * 删除整个索引库
     */
    @Test
    public void deleteIndex() {
        client.admin().indices().prepareDelete("indexsearch").execute().actionGet();
    }

    /**
     * 关闭ES客户端
     */
    @AfterEach
    public void closeClient() {
        client.close();
    }
}
