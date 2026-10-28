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
	/** The {@code elasticsearch.snapshot.repository} property: the kind of repository of the backups, {@code fs} or {@code gcs}. */
	elasticsearch_snapshot_repository("elasticsearch.snapshot.repository"),
	/** The path of the snapshot repositories. */
	SNAPSHOT_PATH("/_snapshot/"),
	/** The parameter that makes the snapshot request wait for the end of the snapshot. */
	WAIT_FOR_COMPLETION("?wait_for_completion=true"),
	/** The pattern of the name of a backup: {@code backup-} plus the date and time, to the millisecond, it started. */
	SNAPSHOT_NAME_FORMAT("'backup-'yyyyMMdd-HHmmss-SSS"),
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
		properties,
		/** The type of a snapshot repository. */
		type,
		/** The settings of a snapshot repository. */
		settings,
		/** The folder of a repository of type {@code fs}. */
		location,
		/** The bucket of a repository of type {@code gcs}. */
		bucket,
		/** The indexes of a snapshot. */
		indices,
		/** Whether a snapshot skips the indexes that do not exist. */
		ignore_unavailable,
		/** Whether a snapshot also keeps the state of the cluster. */
		include_global_state,
		/** The details of a snapshot in the response. */
		snapshot,
	}
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
