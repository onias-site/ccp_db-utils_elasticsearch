package com.ccp.implementations.db.utils.elasticsearch;

import java.util.function.Consumer;

import com.ccp.decorators.CcpFileDecorator;
import com.ccp.especifications.db.utils.entity.fields.CcpErrorDbUtilsIncorrectEntityFields;

/**
 * Handler of {@code ElasticSearchDbRequester.createTables} for an entity whose fields do not match its script: appends
 * the message of the error to the file of the mapping errors.
 */
class ElasticSearchMappingErrorWriter implements Consumer<CcpErrorDbUtilsIncorrectEntityFields> {

	/** The file that receives the mapping errors. */
	private final CcpFileDecorator mappingErrorsFile;

	/**
	 * Keeps the file of the mapping errors.
	 * @param mappingErrorsFile the file that receives the mapping errors
	 */
	ElasticSearchMappingErrorWriter(CcpFileDecorator mappingErrorsFile) {
		this.mappingErrorsFile = mappingErrorsFile;
	}

	/**
	 * Appends the message of the error to the file.
	 * @param error the mapping error
	 */
	public void accept(CcpErrorDbUtilsIncorrectEntityFields error) {
		String message = error.getMessage();
		this.mappingErrorsFile.append(message);
	}
}
