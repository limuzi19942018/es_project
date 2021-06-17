package com.yongli;

import com.yongli.config.EsConfig;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

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

}
