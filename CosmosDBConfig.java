package com.config;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient.Builder;

import lombok.extern.log4j.Log4j2;

@Configuration
@Log4j2
public class CosmosDBConfig {

	@Value("${azure.cosmosdb.uri}")
	private String uri;

	@Value("${azure.cosmosdb.key}")
	private String key;

	@Value("${azure.cosmosdb.database}")
	private String dbName;

	@Value("${azure.cosmosdb.collection}")
	private String dbCollection;

	private Builder clientBuilder;

	private AsyncDocumentClient documentClient;

	public CosmosDBConfig() {
		this.clientBuilder = new Builder();
		log.info("Initialized Cosmos DB client builder!!");
		disableNettyLogs();
	}

	@PostConstruct
	public void init() {
		if (documentClient == null) {
			List<String> preferredLoc = new ArrayList<>();
			preferredLoc.add("Central US");
			ConnectionPolicy policy = ConnectionPolicy.GetDefault();
			policy.setConnectionMode(ConnectionMode.Gateway);
			// policy.setConnectionMode(ConnectionMode.DirectHttps);
			policy.setMaxPoolSize(10);
			policy.setIdleConnectionTimeoutInMillis(6000);
			policy.setMediaRequestTimeoutInMillis(5000);
			policy.setPreferredLocations(preferredLoc);
			documentClient = clientBuilder.withServiceEndpoint(uri).withMasterKeyOrResourceToken(key)
					.withConnectionPolicy(policy).withConsistencyLevel(ConsistencyLevel.BoundedStaleness).build();
			log.info("Initialized Cosmos DB document client!!");
		}
	}

	@Bean("asyncDocumentClient")
	public AsyncDocumentClient getAsyncClient() {
		return documentClient;
	}
	
	private void disableNettyLogs() {
		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
	    LoggerConfig loggerCfg = config.getLoggerConfig("io.netty");
	    loggerCfg.setLevel(Level.OFF);
	    ctx.updateLoggers();
	    log.info("Disabled io.netty logger");
	}

}
