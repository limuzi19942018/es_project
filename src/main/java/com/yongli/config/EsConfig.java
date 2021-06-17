package com.yongli.config;


import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsConfig {



    @Bean
    public RestHighLevelClient restHighLevelClient(){
        //http端口号是9200，也就是服务连接的端口号，此处的9301是用docker部署的映射
        //tcp端口号是9300
        return new RestHighLevelClient
                (RestClient.builder(new HttpHost("10.20.31.205",9301,"http")));
    }
}
