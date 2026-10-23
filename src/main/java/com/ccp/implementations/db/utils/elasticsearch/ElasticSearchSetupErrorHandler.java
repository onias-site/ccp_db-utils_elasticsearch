package com.ccp.implementations.db.utils.elasticsearch;

import java.util.function.Consumer;

/**
 * Handler of {@code ElasticSearchDbRequester.createTables} for any other error of the setup: a
 * {@code ClassNotFoundException} is ignored, anything else aborts the setup.
 */
class ElasticSearchSetupErrorHandler implements Consumer<Throwable> {

	/** The only instance: the handler keeps no state. */
	public final static ElasticSearchSetupErrorHandler INSTANCE = new ElasticSearchSetupErrorHandler();

	private ElasticSearchSetupErrorHandler() {
	}

	/**
	 * Ignores a {@code ClassNotFoundException} and aborts on anything else.
	 * @param error the error of the setup
	 */
	public void accept(Throwable error) {
		boolean isClassNotFoundException = error instanceof ClassNotFoundException;

		if (isClassNotFoundException) {
			return;
		}
		CcpErrorElasticSearchDbSetupUnexpected ccpErrorElasticSearchDbSetupUnexpected = new CcpErrorElasticSearchDbSetupUnexpected(error);
		throw ccpErrorElasticSearchDbSetupUnexpected;
	}

	/** Raised when the database setup meets an unexpected error. */
	@SuppressWarnings("serial")
	private static class CcpErrorElasticSearchDbSetupUnexpected extends RuntimeException {
		/**
		 * Wraps the cause.
		 * @param cause the original failure
		 */
		private CcpErrorElasticSearchDbSetupUnexpected(Throwable cause) {
			super(cause);
		}
	}
}
