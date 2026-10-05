package com.ccp.implementations.db.utils.elasticsearch;

import com.ccp.decorators.CcpJsonFieldName;


/**
 * Property and header names whose real value contains a character that a Java identifier cannot have: legal name in
 * the constant, real value in the constructor, exposed by {@code getValue()}.
 */
enum ElasticSearchDbRequesterSpecialWords implements CcpJsonFieldName{
	/** The {@code elasticsearch.address} property. */
	elasticsearch_address("elasticsearch.address"),
	/** The {@code elasticsearch.secret} property. */
	elasticsearch_secret("elasticsearch.secret"),
	/** The {@code Content-Type} header. */
	Content_Type("Content-Type"),
;
	/** Fields of the connection details and of the index scripts. */
	static enum JsonFieldNames implements CcpJsonFieldName{
		/** The database URL. */
		DB_URL,
		/** The mappings of an index script. */
		mappings,
		/** The dynamic mapping mode. */
		dynamic,
		/** The field mappings. */
		properties}
	/** The real name. */
	private final String value;
	
	/**
	 * Associates the constant with its real name.
	 * @param value the real name
	 */
	private ElasticSearchDbRequesterSpecialWords(String value) {
		this.value = value;
	}

	/**
	 * Returns the real name.
	 * @return the real name
	 */
	public String getValue() {
		return this.value;
	}
}
