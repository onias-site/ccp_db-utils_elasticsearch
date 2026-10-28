package com.ccp.implementations.db.utils.elasticsearch;

import com.ccp.constants.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.implementations.db.utils.elasticsearch.ElasticSearchDbRequesterSpecialWords.JsonFieldNames;

/**
 * The kinds of snapshot repository the backup of {@code ElasticSearchDbRequester} registers, named by the property
 * {@code elasticsearch.snapshot.repository} ({@code fs} when it is absent). The repository is named after the bucket,
 * and Elasticsearch writes the snapshot straight into it.
 */
enum ElasticSearchSnapshotRepositoryType {
	/**
	 * A folder of the machine of Elasticsearch: the bucket is a folder under the {@code path.repo} of the node (on this
	 * machine, {@code C:\jn\snapshots}).
	 */
	fs{
		/**
		 * The folder of the bucket, relative to {@code path.repo}.
		 * @param bucketName the bucket
		 * @return the settings
		 */
		CcpJsonRepresentation getSettings(String bucketName) {
			CcpJsonRepresentation settings = CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.location, bucketName);
			return settings;
		}
	},
	/**
	 * A bucket of Google Cloud Storage, written by the plugin {@code repository-gcs} of the nodes with the credentials of
	 * their keystore.
	 */
	gcs{
		/**
		 * The bucket of Google Cloud Storage.
		 * @param bucketName the bucket
		 * @return the settings
		 */
		CcpJsonRepresentation getSettings(String bucketName) {
			CcpJsonRepresentation settings = CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.bucket, bucketName);
			return settings;
		}
	},
	;

	/**
	 * The settings of the repository of the bucket.
	 * @param bucketName the bucket
	 * @return the settings
	 */
	abstract CcpJsonRepresentation getSettings(String bucketName);

	/**
	 * The body that registers the repository of the bucket: its type and its settings.
	 * @param bucketName the bucket
	 * @return the body of {@code PUT _snapshot/<bucket>}
	 */
	CcpJsonRepresentation getRepository(String bucketName) {
		CcpJsonRepresentation settings = this.getSettings(bucketName);
		String typeName = this.name();
		CcpJsonRepresentation repositoryWithType = CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.type, typeName);
		CcpJsonRepresentation repository = repositoryWithType.put(JsonFieldNames.settings, settings);
		return repository;
	}
}
