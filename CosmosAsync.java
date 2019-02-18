package com.repo;

import java.util.concurrent.CompletableFuture;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Async;

import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.PartitionKey;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.internal.NotFoundException;

import lombok.extern.log4j.Log4j2;
import rx.Observable;

@Log4j2
public class CosmosRepositoryImpl  {

	@Autowired
	private AsyncDocumentClient asyncDocumentClient;

	private String collectionPath;

	@Value("${azure.cosmosdb.database}")
	private String database;

	@Value("${azure.cosmosdb.collection}")
	private String collection;

	@PostConstruct
	public void init() {
		collectionPath = String.format("/dbs/%s/colls/%s", database, collection);
		log.info("CollectionPath: " + collectionPath + " client:" + asyncDocumentClient.getServiceEndpoint());
	}

	@Override
	@Async("threadPoolTaskExecutor")
	public CompletableFuture<Item> getInvObj(ItemRequestBean invObj) {
		String key = invObj.generateKey();
		Item item = null;
		long startTime = System.currentTimeMillis();
		if(key!=null) {
			try {
				Observable<Item> observableItem = asyncDocumentClient
						.readDocument(getDocumentLink(key), getRequestOptions(key)).map(ResourceResponse::getResource)
						.map(doc -> {
							return doc.toObject(Item.class);
						});
				item = observableItem.toBlocking().first();
			} catch (RuntimeException e) {
				String sErrorCause = e.getCause().toString();
				if (sErrorCause.contains("Entity with the specified id does not exist in the system")) {
					log.error("Record not found for request " + invObj);
				} else {
					throw new RuntimeException(e);
				}
			}
			log.info("Time taken to fetch from cosmosDB: "+ (System.currentTimeMillis()-startTime));
		}
		return CompletableFuture.completedFuture(item);
	}

	@Override
	public void upsertItem(Item item) {
		item.setIdValue();
		log.debug("key: " + item.getId());
		asyncDocumentClient
				.upsertDocument(collectionPath, item, getRequestOptions(null), true).single().toCompletable().await();

	}

	@Override
	public void deleteItem(ItemRequestBean invObj) {
		String id = invObj.generateKey();
		log.debug("Key : " + id);
		String selfLink = asyncDocumentClient.readDocument(getDocumentLink(id), getRequestOptions(id)).single()
				.map(ResourceResponse::getResource).toBlocking().first().getSelfLink();
		Observable<ResourceResponse<Document>> observableResp = asyncDocumentClient
				.deleteDocument(selfLink, getRequestOptions(id)).single();
		Observable<Boolean> observableFlag = observableResp.map(ResourceResponse::getStatusCode).switchMap(status -> {
			if (404 == status) {
				return Observable.error(new NotFoundException("Could not find document."));
			} else if (204 == status) {
				return Observable.just(true);
			} else {
				return Observable.just(false);
			}
		}).onErrorResumeNext(e -> {
			return Observable.error(new Exception("Error deleting document.", e));
		}).switchIfEmpty(Observable.error(new NotFoundException("Could not find document.")));
		log.debug(observableFlag.toBlocking().first());
	}

	private String getDocumentLink(String id) {
		return collectionPath + "/docs/" + id;
	}

	@Bean
	public RequestOptions getRequestOptions(String partitionKey) {
		RequestOptions reqOptions = new RequestOptions();
		if (partitionKey != null) {
			reqOptions.setPartitionKey(new PartitionKey(partitionKey));
		}
		return reqOptions;
	}

	@Bean
	public FeedOptions getFeedOptions() {
		FeedOptions feedOptions = new FeedOptions();
		feedOptions.setEnableCrossPartitionQuery(true);
		return feedOptions;
	}
}
