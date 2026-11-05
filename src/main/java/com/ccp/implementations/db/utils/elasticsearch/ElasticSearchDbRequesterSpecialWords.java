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
	/** The start of the path of an index. */
	INDEX_PATH_START("/"),
	/** What makes an index show what was just written. */
	REFRESH_PATH("/_refresh"),
	/** The name of the refresh in the logs of the database. */
	REFRESH_TRACE("refreshEntity"),
	/** The W3C header that carries the trace id; Elasticsearch writes it as {@code trace.id} in the slowlogs. */
	traceparent("traceparent"),
	/** The header that identifies one call; Elasticsearch writes it as {@code elasticsearch.slowlog.id} in the search slowlog. */
	X_Opaque_Id("X-Opaque-Id"),
	/** The version that opens a {@code traceparent}. */
	TRACEPARENT_VERSION("00-"),
	/** The flag (sampled) that closes a {@code traceparent}. */
	TRACEPARENT_SAMPLED("-01"),
	/** The separator of the parts of a {@code traceparent} and of an {@code X-Opaque-Id}. */
	TRACE_SEPARATOR("-"),
	/** How a response that reports its own time starts (search, bulk). */
	TOOK_START("{\"took\":"),
	/** What ends the number of {@code took}. */
	TOOK_END(","),
	/** An index named in the body ({@code _mget}, {@code _bulk}): the group 1 is the index. */
	INDEX_IN_THE_BODY("\"_index\"\\s*:\\s*\"([^\"]+)\""),
	/** What ends the indexes of a path ({@code /<index1>,<index2>/<endpoint>}) or starts its query string. */
	PATH_SEGMENT_END("[/?]"),
	/** What separates the indexes of a path. */
	INDEX_SEPARATOR(","),
	/** What starts an endpoint instead of an index ({@code /_mget}, {@code /_bulk}, {@code /_cat}). */
	ENDPOINT_START("_"),
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
		/** The HTTP method of a measured call. */
		method,
		/** The path of a measured call, after the address of the database. */
		path,
		/** The HTTP status of a measured call. */
		status,
		/** The size of the body sent in a measured call. */
		requestBytes,
		/** The size of the body received in a measured call. */
		responseBytes,
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
