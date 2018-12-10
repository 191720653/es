package com.es.config;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsConfiguration {

	private static String hosts = "127.0.0.1";
	private static int port = 9200;
	private static String schema = "http";
	private static ArrayList<HttpHost> hostList = null;

	private static int connectTimeOut = 1000;
	private static int socketTimeOut = 30000;
	private static int connectionRequestTimeOut = 500;

	private static int maxConnectNum = 100;
	private static int maxConnectPerRoute = 100;

	private RestClientBuilder builder;
	private RestHighLevelClient client;

	static {
		hostList = new ArrayList<>();
		String[] hostStrs = hosts.split(",");
		for (String host : hostStrs) {
			hostList.add(new HttpHost(host, port, schema));
		}
	}

	@Bean
	public RestHighLevelClient client() {
		builder = RestClient.builder(hostList.toArray(new HttpHost[0]));
		setConnectTimeOutConfig();
		setMutiConnectConfig();
		client = new RestHighLevelClient(builder);
		return client;
	}

	// 异步httpclient的连接延时配置
	public void setConnectTimeOutConfig() {
		builder.setRequestConfigCallback(new RequestConfigCallback() {

			@Override
			public Builder customizeRequestConfig(Builder requestConfigBuilder) {
				requestConfigBuilder.setConnectTimeout(connectTimeOut);
				requestConfigBuilder.setSocketTimeout(socketTimeOut);
				requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeOut);
				return requestConfigBuilder;
			}
		});
	}

	// 异步httpclient的连接数配置
	public void setMutiConnectConfig() {
		builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {

			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				httpClientBuilder.setMaxConnTotal(maxConnectNum);
				httpClientBuilder.setMaxConnPerRoute(maxConnectPerRoute);
				return httpClientBuilder;
			}
		});
	}

	public void close() {
		if (client != null) {
			try {
				client.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}