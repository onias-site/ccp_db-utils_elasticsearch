package com.ccp.implementations.db.utils.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.db.utils.CcpDbRequester;

/**
 * DI provider that exposes {@code ElasticSearchDbRequester} as the {@code CcpDbRequester} implementation.
 */
public class CcpElasticSearchDbRequest implements CcpInstanceProvider<CcpDbRequester> {

	/**
	 * Builds the Elasticsearch implementation of {@code CcpDbRequester}.
	 * @return a new {@code ElasticSearchDbRequester}
	 */
	public CcpDbRequester getInstance() {
		ElasticSearchDbRequester elasticSearchDbRequester = new ElasticSearchDbRequester();
		return elasticSearchDbRequester;
	}

}
