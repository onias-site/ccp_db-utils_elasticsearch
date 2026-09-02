package com.ccp.implementations.db.utils.elasticsearch;

import com.ccp.decorators.CcpJsonFieldName;

enum ElasticSearchDbRequesterSpecialWords implements CcpJsonFieldName{
	elasticsearch_address("elasticsearch.address"),
	elasticsearch_secret("elasticsearch.secret"),
	Content_Type("Content-Type"),
;
	static enum JsonFieldNames implements CcpJsonFieldName{
		Accept, DB_URL, mappings, dynamic, properties, Authorization
		
	}
	private final String value;
	
	private ElasticSearchDbRequesterSpecialWords(String value) {
		this.value = value;
	}

	public String getValue() {
		return this.value;
	}
}
