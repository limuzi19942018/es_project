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
        return new RestHighLevelClient
                (RestClient.builder(new HttpHost("10.20.31.205",9302,"http")));
    }
}
