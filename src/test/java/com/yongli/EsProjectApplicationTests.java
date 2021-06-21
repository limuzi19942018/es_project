package com.yongli;

import com.alibaba.fastjson.JSON;
import com.yongli.config.EsConfig;
import com.yongli.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SpringBootTest
class EsProjectApplicationTests {


	@Autowired
	private RestHighLevelClient client;


	/**
	 * 创建一个索引
	 * @throws Exception
	 */
	@Test
	void createIndex() throws Exception{
		//创建索引请求
		CreateIndexRequest request = new CreateIndexRequest("test_index");
		//执行请求
		CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
		System.out.println(response);
	}


	/**
	 * 判断es库里面的索引是否存在
	 * @throws Exception
	 */
	@Test
	void testExistIndex()throws Exception{
		GetIndexRequest request = new GetIndexRequest("test_index");
		boolean b = client.indices().exists(request, RequestOptions.DEFAULT);
		System.out.println(b);
	}

	/**
	 * 删除索引
	 * @throws Exception
	 */
	@Test
	void testDeleteIndex()throws Exception{
		DeleteIndexRequest request = new DeleteIndexRequest("label2");
		AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
		System.out.println(response.isAcknowledged());
	}


	/**
	 * 插入一个文档对象
	 * @throws Exception
	 */
	@Test
	void testInsertDocument()throws Exception{
		User user = new User("李四", 2);
		//创建请求
		IndexRequest request = new IndexRequest("test_index");
		//可以指定文档的id,如果不指定，es会自动生成一个
		//request.id("1");
		//设置响应时间（5s）
		request.timeout(TimeValue.timeValueSeconds(5));
		//将对象数据放入请求种去
		request.source(JSON.toJSONString(user), XContentType.JSON);
		//执行请求
		IndexResponse response = client.index(request, RequestOptions.DEFAULT);
		System.out.println(response);
	}


	/**
	 * 判断文档是否存在（根据索引和id判断）
	 * @throws Exception
	 */
	@Test
	void testExistDocument()throws Exception{
		GetRequest request = new GetRequest("test_index", "1");
		boolean b = client.exists(request, RequestOptions.DEFAULT);
		System.out.println(b);
	}


	/**
	 * 获取文档，根据id和索引
	 * @throws Exception
	 */
	@Test
	void testGetDocument()throws Exception{
		GetRequest request = new GetRequest("test_index", "1");
		GetResponse response = client.get(request, RequestOptions.DEFAULT);
		//把返回的文档对象，转成map
		Map<String, Object> sourceAsMap = response.getSourceAsMap();
		System.out.println(sourceAsMap);
	}


	/**
	 * 更新一个文档
	 * @throws Exception
	 */
	@Test
	void testUpdateDocument()throws Exception{
		UpdateRequest request = new UpdateRequest("test_index", "1");
		User newUser = new User("王二麻子，你是个有名字的人，你的小明叫老王", 23);
		request.doc(JSON.toJSONString(newUser), XContentType.JSON);
		request.timeout("5s");
		UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
		System.out.println(update.status());

	}

	/**
	 * 删除文档
	 * @throws Exception
	 */
	@Test
	void testDeleteDocument()throws Exception{
		DeleteRequest request = new DeleteRequest("test_index", "1");
		request.timeout("5s");
		DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
		System.out.println(delete.status());
	}

	@Test
	void testBulkInsertDocument()throws Exception{
		BulkRequest request = new BulkRequest();
		List<User> list = new ArrayList<>();
		User jack = new User("王二不麻子", 11);
		User rose = new User("rosdde", 22);
		User maria = new User("赵不云", 33);
		User linda = new User("单独linda", 4);
		User blue = new User("刘三备", 5);
		User origin = new User("origin", 6);
		User apple = new User("曹操", 7);
		list.add(jack);
		list.add(rose);
		list.add(maria);
		list.add(linda);
		list.add(blue);
		list.add(origin);
		list.add(apple);
		for (int index=0;index<list.size();index++) {
			request.add(new IndexRequest("test_index")
					.source(JSON.toJSONString(list.get(index)),XContentType.JSON));
		}
		BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);
		System.out.println(bulk.hasFailures());

	}


	/**
	 * 查询文档
	 * @throws Exception
	 */
	@Test
	void testSearchDocument()throws Exception{
		//SearchRequest request = new SearchRequest("news_info");
		SearchRequest request = new SearchRequest("test_index");
		//查询对象
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		//查询构造器
		sourceBuilder.timeout(TimeValue.timeValueSeconds(10));
		//全部匹配，会查询出所有
		//MatchAllQueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		//sourceBuilder.query(queryBuilder);

		//termQuery不会对搜索词进行分词处理，而是作为一个整体与目标字段进行匹配，若完全匹配，则可查询到。如果查询的自动里面有中文，则需要加上kewword
		//TermQueryBuilder term = QueryBuilders.termQuery("userName.keyword", "王二麻子");
		//TermQueryBuilder term = QueryBuilders.termQuery("age", 1);
		//sourceBuilder.query(term);

		//matchQuery会将搜索词分词，再与目标查询字段进行匹配，若分词中的任意一个词与目标字段匹配上，则可查询到。
		//MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("userName", "王二");
		//sourceBuilder.query(matchQuery);

		//以某某关键字开头
		//PrefixQueryBuilder prefix = QueryBuilders.prefixQuery("userName", "李");
		//sourceBuilder.query(prefix);

		//range范围的使用 设置范围，比如设置年龄范围从1-4岁
		RangeQueryBuilder range = QueryBuilders.rangeQuery("age").from(1).to(4);
		sourceBuilder.query(range);
		//从第几条开始(from的值计算方式： （当前页码-1）*每页需要展示的条数  ，当前页码是前端传递过来的   )
		//sourceBuilder.from(2);
		//返回的条数
		//sourceBuilder.size(50);

		//设置高亮
		HighlightBuilder highlightBuilder = new HighlightBuilder();
		//设置高亮字段
		highlightBuilder.field("age");
		//设置起始标签
		highlightBuilder.preTags("<span style='color:red'");
		//设置结束标签
		highlightBuilder.postTags("</span>");
		sourceBuilder.highlighter(highlightBuilder);

		request.source(sourceBuilder);
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		SearchHit[] _hits = response.getHits().getHits();
		for (SearchHit hit : _hits) {
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			//获取高亮设置的对象
			Map<String, HighlightField> highlightFields = hit.getHighlightFields();
			if(highlightFields!=null && highlightFields.size()>0){
				//获取上文设置的高亮的字段
				HighlightField userName = highlightFields.get("age");
				Text[] fragments = userName.getFragments();
				//把高亮的信息拼接
				String newUserName="";
				for (Text fragment : fragments) {
					newUserName+=fragment;
				}
				//重新封装map对象
				sourceAsMap.put("age",newUserName);
			}
			System.out.println(sourceAsMap);
			// 最终打印的结果为：把关键词 王二 高亮了
			// {userName=<span style='color:red'王</span><span style='color:red'二</span>麻子, age=1}
		}

	}


	/**
	 * 组合查询一：must的用法
	 *
	 * must 相当于 and
	 * should 相当于 or
	 * mustNot 相当于 not
	 */
	@Test
	void testZuHeSearchDocument1()throws Exception{
		//构建查询请求
		SearchRequest request = new SearchRequest("test_index");
		//查询对象
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//查询构造器(组合查询，查询userName 为王二，并且age 为1的人)
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		boolQueryBuilder.must(QueryBuilders.matchQuery("userName","王二"));
		boolQueryBuilder.must(QueryBuilders.termQuery("age",11));

		searchSourceBuilder.query(boolQueryBuilder);
		request.source(searchSourceBuilder);
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit hit : hits) {
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			System.out.println(sourceAsMap);
		}
	}



	/**
	 * 组合查询二： should的用法
	 *
	 * must 相当于 and
	 * should 相当于 or
	 * mustNot 相当于 not
	 */
	@Test
	void testZuHeSearchDocument2()throws Exception{
		//构建查询请求
		SearchRequest request = new SearchRequest("test_index");
		//查询对象
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//查询构造器(组合查询，查询userName 为王二，并且age 为1的人)
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		boolQueryBuilder.should(QueryBuilders.matchQuery("userName","刘备"));
		boolQueryBuilder.should(QueryBuilders.matchQuery("userName","王二"));
		searchSourceBuilder.query(boolQueryBuilder);
		request.source(searchSourceBuilder);
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit hit : hits) {
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			System.out.println(sourceAsMap);
		}
	}


	/**
	 * 聚合查询(查询年龄种最大的)
	 * @throws Exception
	 */
	@Test
	void testJuHeSearch()throws Exception{
		//构建查询请求
		SearchRequest request = new SearchRequest("test_index");
		//查询对象
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//聚合查询，查询年龄最大的数据
		MaxAggregationBuilder aggregaBuilder = AggregationBuilders.max("maxAge").field("age");
		searchSourceBuilder.aggregation(aggregaBuilder);
		request.source(searchSourceBuilder);
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		//打印response时，会把最大年龄查询出来：
		//"aggregations":{"max#maxAge":{"value":33.0}}}
		System.out.println(response);
		//结果会展示所有数据，单独的最大年龄的数据会放到一个叫aggregations的对象里面
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit hit : hits) {
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			System.out.println(sourceAsMap);
		}
	}


	/**
	 * 范围查询
	 * @throws Exception
	 */
	@Test
	void rangeSearch()throws Exception{
		//构建查询请求
		SearchRequest request = new SearchRequest("test_index");
		//查询对象
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//范围查询，查询年龄在22-23之间的
		RangeQueryBuilder range = QueryBuilders.rangeQuery("age").gt(10).lt(23);
		searchSourceBuilder.query(range);
		request.source(searchSourceBuilder);
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		//打印response时，会把最大年龄查询出来：
		//"aggregations":{"max#maxAge":{"value":33.0}}}
		System.out.println(response);
		//结果会展示所有数据，单独的最大年龄的数据会放到一个叫aggregations的对象里面
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit hit : hits) {
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			System.out.println(sourceAsMap);
		}
	}


	/**
	 * 模糊查询
	 * @throws Exception
	 */
	@Test
	void fuzzySearch()throws Exception{
		//构建查询请求
		SearchRequest request = new SearchRequest("test_index");
		//查询对象
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		//范围查询，查询年龄在22-23之间的
		FuzzyQueryBuilder fuzzy = QueryBuilders.fuzzyQuery("userName", "王二麻子").fuzziness(Fuzziness.ONE);
		searchSourceBuilder.query(fuzzy);
		request.source(searchSourceBuilder);
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		//打印response时，会把最大年龄查询出来：
		//"aggregations":{"max#maxAge":{"value":33.0}}}
		System.out.println(response);
		//结果会展示所有数据，单独的最大年龄的数据会放到一个叫aggregations的对象里面
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit hit : hits) {
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			System.out.println(sourceAsMap);
		}
	}

}
