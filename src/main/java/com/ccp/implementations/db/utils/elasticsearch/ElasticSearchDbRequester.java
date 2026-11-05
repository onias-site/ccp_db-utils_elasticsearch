package com.ccp.implementations.db.utils.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.ccp.constants.CcpOtherConstants;
import com.ccp.decorators.CcpCollectionDecorator;
import com.ccp.decorators.CcpFileDecorator;
import com.ccp.decorators.CcpFolderDecorator;
import com.ccp.decorators.CcpErrorInputStreamMissing;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpPropertiesDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.decorators.CcpTimeDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpBulkExecutor;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;
import com.ccp.especifications.db.utils.CcpDbCallsOfTheThread;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.http.CcpHttpResponse;
import com.ccp.especifications.db.utils.entity.CcpEntity;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityFactory;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityMetaData;
import com.ccp.especifications.db.utils.entity.fields.CcpEntityField;
import com.ccp.especifications.db.utils.entity.fields.CcpErrorDbUtilsIncorrectEntityFields;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.especifications.http.CcpHttpMethods;
import com.ccp.especifications.http.CcpHttpRequester;
import com.ccp.especifications.http.CcpHttpResponseTransform;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.implementations.db.utils.elasticsearch.ElasticSearchDbRequesterSpecialWords.JsonFieldNames;
import java.util.stream.Stream; 




import com.ccp.json.fields.validation.CcpJsonCommonsFields;

/**
 * {@code CcpDbRequester} implementation for Elasticsearch. Reads the connection properties
 * ({@code elasticsearch.address} / {@code elasticsearch.secret}) and runs HTTP requests against
 * the cluster. Also provides {@code executeDatabaseSetup} to recreate indexes and insert the initial
 * records from mapping scripts.
 */
class ElasticSearchDbRequester implements CcpDbRequester {

	/** The connection details, loaded once: {@code DB_URL}, {@code Authorization}, {@code Content-Type} and {@code Accept}. */
	private CcpJsonRepresentation connectionDetails = CcpOtherConstants.EMPTY_JSON;
	/**
	 * The headers of every request: the connection details without {@code DB_URL}. Until 2026-10-07 the connection
	 * details were sent as they are, so the address of the database went as an HTTP header in every request.
	 */
	private CcpJsonRepresentation headers = CcpOtherConstants.EMPTY_JSON;

	/** Finds the indexes named in a body ({@code "_index"} of each item of an {@code _mget} or a {@code _bulk}). */
	private static final Pattern INDEX_IN_THE_BODY = Pattern.compile(ElasticSearchDbRequesterSpecialWords.INDEX_IN_THE_BODY.getValue());

	/** The kind of repository of the backups, from {@code elasticsearch.snapshot.repository} ({@code fs} when absent). */
	private ElasticSearchSnapshotRepositoryType snapshotRepositoryType = ElasticSearchSnapshotRepositoryType.fs;
	
	/**
	 * Loads, once, the connection details from {@code application_properties} (environment variable, classpath or file):
	 * {@code elasticsearch.address} (default {@code http://localhost:9200}) becomes {@code DB_URL} and
	 * {@code elasticsearch.secret} (default empty) becomes {@code Authorization}.
	 * @return this requester
	 */
	private CcpDbRequester loadConnectionProperties() {
		boolean connectionDetailsEmpty = this.connectionDetails.isEmpty();
		boolean alreadyLoaded = false == connectionDetailsEmpty;
		if(alreadyLoaded) {
			return this;
		}
		CcpJsonRepresentation systemProperties;
		try {
			CcpStringDecorator propertiesFileName = new CcpStringDecorator("application_properties");
			CcpPropertiesDecorator propertiesDecorator = propertiesFileName.propertiesFrom();
			systemProperties = propertiesDecorator.environmentVariablesOrClassLoaderOrFile();
		} catch (CcpErrorInputStreamMissing e) {
			CcpJsonRepresentation defaultProperties = CcpOtherConstants.EMPTY_JSON
					.put(ElasticSearchDbRequesterSpecialWords.elasticsearch_address, "http://localhost:9200");
					systemProperties = defaultProperties
					.put(ElasticSearchDbRequesterSpecialWords.elasticsearch_secret, "")
					;
		}
		String defaultRepositoryType = ElasticSearchSnapshotRepositoryType.fs.name();
		String repositoryType = systemProperties.getOrDefault(ElasticSearchDbRequesterSpecialWords.elasticsearch_snapshot_repository, () -> defaultRepositoryType);
		this.snapshotRepositoryType = ElasticSearchSnapshotRepositoryType.valueOf(repositoryType);
		CcpJsonRepresentation propertiesWithAddress = systemProperties
				.putIfNotContains(ElasticSearchDbRequesterSpecialWords.elasticsearch_address, "http://localhost:9200");

				CcpJsonRepresentation propertiesWithAddressAndSecret = propertiesWithAddress
				.putIfNotContains(ElasticSearchDbRequesterSpecialWords.elasticsearch_secret, "");
				CcpJsonRepresentation addressAndSecret = propertiesWithAddressAndSecret.getJsonPiece(ElasticSearchDbRequesterSpecialWords.elasticsearch_address, ElasticSearchDbRequesterSpecialWords.elasticsearch_secret);
				CcpJsonRepresentation propertiesWithDbUrl = addressAndSecret
				.renameField(ElasticSearchDbRequesterSpecialWords.elasticsearch_address, JsonFieldNames.DB_URL);

				CcpJsonRepresentation propertiesWithAuthorization = propertiesWithDbUrl.renameField(ElasticSearchDbRequesterSpecialWords.elasticsearch_secret, CcpJsonCommonsFields.Authorization)
				;
				CcpJsonRepresentation propertiesWithContentType = propertiesWithAuthorization
				.put(ElasticSearchDbRequesterSpecialWords.Content_Type, "application/json");

				this.connectionDetails = propertiesWithContentType
				.put(CcpJsonCommonsFields.Accept, "application/json")
				;
		this.headers = this.connectionDetails.removeFields(JsonFieldNames.DB_URL);
		return this;
	}

	
	/**
	 * Runs a request with a text body, merging the given headers over the connection headers.
	 * @param <V> the result type
	 * @param trace identifier of the request in error messages
	 * @param url the path after the database URL
	 * @param method the HTTP method
	 * @param expectedStatus the only accepted status
	 * @param body the request body
	 * @param headers additional headers
	 * @param transformer turns the response into the result
	 * @return the result
	 */
	public <V> V executeHttpRequest(String trace, String url, CcpHttpMethods method,  Integer expectedStatus, String body, CcpJsonRepresentation headers, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();;
		CcpJsonRepresentation mergedHeaders = this.headers.mergeWithAnotherJson(headers);
		String dbUrl = this.connectionDetails.getAsString(JsonFieldNames.DB_URL);
		String path = dbUrl + url;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, path);
		V response = this.executeMeasured(trace, url, path, method, http, mergedHeaders, body, transformer);
		return response;
	}

	/**
	 * Runs the request and records it in the unit of the current thread ({@link CcpDbCallsOfTheThread}): when a unit runs,
	 * the request carries its trace ({@code traceparent}) and the id of the call ({@code X-Opaque-Id}), which Elasticsearch
	 * writes in its slowlogs, and the call is recorded with the latency seen here and the {@code took} of the response.
	 * The call is recorded before the response is checked, so a refused status is measured too.
	 * @param <V> the result type
	 * @param trace identifier of the request in error messages
	 * @param relativePath the path after the address of the database
	 * @param path the whole URL
	 * @param method the HTTP method
	 * @param http checks the status and routes the response
	 * @param headers the headers of the request
	 * @param body the request body
	 * @param transformer turns the response into the result
	 * @return the result
	 */
	private <V> V executeMeasured(String trace, String relativePath, String path, CcpHttpMethods method, CcpHttpHandler http, CcpJsonRepresentation headers, String body, CcpHttpResponseTransform<V> transformer) {
		boolean notMeasuring = false == CcpDbCallsOfTheThread.isMeasuring();
		if(notMeasuring) {
			V response = http.executeHttpRequest(trace, method, headers, body, transformer);
			return response;
		}
		int sequence = CcpDbCallsOfTheThread.getNextSequence();
		CcpJsonRepresentation headersWithTrace = getHeadersWithTrace(headers, sequence);
		long startedAt = System.nanoTime();
		CcpHttpResponse response = http.ccpHttp.executeHttpRequest(path, method, headersWithTrace, body);
		long finishedAt = System.nanoTime();
		long elapsedNanos = finishedAt - startedAt;
		double elapsedMs = elapsedNanos / 1_000_000.0;
		double clientMs = CcpDbCallsOfTheThread.roundToTenths(elapsedMs);
		String methodName = method.name();
		int requestBytes = body.length();
		String responseBody = response.httpResponse;
		int responseBytes = responseBody.length();
		CcpJsonRepresentation call = CcpOtherConstants.EMPTY_JSON
				.put(CcpDbCallsOfTheThread.Fields.sequence, sequence)
				.put(JsonFieldNames.method, methodName)
				.put(JsonFieldNames.path, relativePath)
				.put(JsonFieldNames.status, response.httpStatus)
				.put(CcpDbCallsOfTheThread.Fields.clientMs, clientMs)
				.put(JsonFieldNames.requestBytes, requestBytes)
				.put(JsonFieldNames.responseBytes, responseBytes);
		List<String> indices = getIndices(relativePath, body);
		CcpJsonRepresentation callWithIndices = call.put(JsonFieldNames.indices, indices);
		CcpJsonRepresentation callWithTook = getCallWithTook(callWithIndices, responseBody);
		CcpDbCallsOfTheThread.record(callWithTook);
		V result = http.executeHttpRequest(trace, method, headersWithTrace, body, transformer, response);
		return result;
	}

	/**
	 * The headers plus the trace of the unit ({@code traceparent}, which the indexing slowlog also keeps) and the id of the
	 * call ({@code X-Opaque-Id}, {@code <trace id>-<sequence>}, which the search slowlog keeps).
	 * @param headers the headers of the request
	 * @param sequence the position of the call in the unit
	 * @return the headers with the trace
	 */
	private static CcpJsonRepresentation getHeadersWithTrace(CcpJsonRepresentation headers, int sequence) {
		String traceId = CcpDbCallsOfTheThread.getTraceId();
		String newId = CcpDbCallsOfTheThread.newTraceId();
		String spanId = newId.substring(0, 16);
		String version = ElasticSearchDbRequesterSpecialWords.TRACEPARENT_VERSION.getValue();
		String separator = ElasticSearchDbRequesterSpecialWords.TRACE_SEPARATOR.getValue();
		String sampled = ElasticSearchDbRequesterSpecialWords.TRACEPARENT_SAMPLED.getValue();
		String traceparent = version + traceId + separator + spanId + sampled;
		String opaqueId = traceId + separator + sequence;
		CcpJsonRepresentation headersWithTrace = headers
				.put(ElasticSearchDbRequesterSpecialWords.traceparent, traceparent)
				.put(ElasticSearchDbRequesterSpecialWords.X_Opaque_Id, opaqueId);
		return headersWithTrace;
	}

	/**
	 * The indexes a call touches: the ones of the path ({@code /<index1>,<index2>/_search}) and the ones named in the body
	 * ({@code "_index"} of each item of an {@code _mget} or of each action of a {@code _bulk}), without repetition, in
	 * alphabetical order. A path that starts with an endpoint ({@code /_mget}, {@code /_cat}) names no index.
	 * @param relativePath the path after the address of the database
	 * @param body the request body
	 * @return the indexes, empty when the call touches none
	 */
	private static List<String> getIndices(String relativePath, String body) {
		List<String> pathIndices = getPathIndices(relativePath);
		Set<String> indices = new TreeSet<>(pathIndices);
		Matcher bodyIndices = INDEX_IN_THE_BODY.matcher(body);
		while (bodyIndices.find()) {
			String bodyIndex = bodyIndices.group(1);
			indices.add(bodyIndex);
		}
		List<String> sortedIndices = new ArrayList<>(indices);
		return sortedIndices;
	}

	/**
	 * The indexes named in the path: its first segment, split by commas, unless it is an endpoint ({@code _mget},
	 * {@code _bulk}, {@code _cat}).
	 * @param relativePath the path after the address of the database
	 * @return the indexes of the path, empty when the path starts with an endpoint
	 */
	private static List<String> getPathIndices(String relativePath) {
		String indexPathStart = ElasticSearchDbRequesterSpecialWords.INDEX_PATH_START.getValue();
		boolean startsWithSlash = relativePath.startsWith(indexPathStart);
		int pathStart = startsWithSlash ? 1 : 0;
		String pathWithoutSlash = relativePath.substring(pathStart);
		String segmentEnd = ElasticSearchDbRequesterSpecialWords.PATH_SEGMENT_END.getValue();
		String[] segments = pathWithoutSlash.split(segmentEnd, 2);
		String firstSegment = segments[0];
		String endpointStart = ElasticSearchDbRequesterSpecialWords.ENDPOINT_START.getValue();
		boolean pathNamesNoIndex = firstSegment.isEmpty() || firstSegment.startsWith(endpointStart);
		if(pathNamesNoIndex) {
			return new ArrayList<>();
		}
		String indexSeparator = ElasticSearchDbRequesterSpecialWords.INDEX_SEPARATOR.getValue();
		String[] pathIndices = firstSegment.split(indexSeparator);
		List<String> pathIndicesList = Arrays.asList(pathIndices);
		return pathIndicesList;
	}

	/**
	 * The call plus the time Elasticsearch reported ({@code took}, in milliseconds), read from the start of the response
	 * without parsing it: only searches and bulks report one, and they open the response with it.
	 * @param call the call
	 * @param responseBody the body of the response
	 * @return the call with {@code tookMs}, or the call as it is when the response reports no time
	 */
	private static CcpJsonRepresentation getCallWithTook(CcpJsonRepresentation call, String responseBody) {
		String tookStart = ElasticSearchDbRequesterSpecialWords.TOOK_START.getValue();
		boolean withoutTook = false == responseBody.startsWith(tookStart);
		if(withoutTook) {
			return call;
		}
		int numberStart = tookStart.length();
		String tookEnd = ElasticSearchDbRequesterSpecialWords.TOOK_END.getValue();
		int numberEnd = responseBody.indexOf(tookEnd, numberStart);
		boolean numberNotClosed = numberEnd < 0;
		if(numberNotClosed) {
			return call;
		}
		String number = responseBody.substring(numberStart, numberEnd);
		try {
			long tookMs = Long.parseLong(number);
			CcpJsonRepresentation callWithTook = call.put(CcpDbCallsOfTheThread.Fields.tookMs, tookMs);
			return callWithTook;
		} catch (NumberFormatException e) {
			return call;
		}
	}

	
	/**
	 * Runs a request against the given indexes ({@code <db>/<index1>,<index2><pathSuffix>}).
	 * @param <V> the result type
	 * @param trace identifier of the request in error messages
	 * @param pathSuffix the path after the indexes
	 * @param method the HTTP method
	 * @param expectedStatus the only accepted status
	 * @param body the request body
	 * @param resources the indexes
	 * @param transformer turns the response into the result
	 * @return the result
	 */
	public <V> V executeHttpRequest(String trace, String pathSuffix, CcpHttpMethods method, Integer expectedStatus, CcpJsonRepresentation body,  String[] resources, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();
		String dbUrl = this.connectionDetails.getAsString(JsonFieldNames.DB_URL);
		String dbUrlWithSlash = dbUrl + "/";
		Stream<String> resourcesStream = Arrays.asList(resources).stream();
		List<String> resourcesList = resourcesStream
				.collect(Collectors.toList());
				String resourcesAsText = resourcesList
				.toString();
				String resourcesWithoutOpeningBracket = resourcesAsText
				.replace("[", "");
				String resourcesWithoutBrackets = resourcesWithoutOpeningBracket.replace("]", "");
				String commaSeparatedResources = resourcesWithoutBrackets.replace(" ", "");
				String dbUrlWithResources = dbUrlWithSlash +  commaSeparatedResources;
				String path = dbUrlWithResources + pathSuffix;
		String indexPathStart = ElasticSearchDbRequesterSpecialWords.INDEX_PATH_START.getValue();
		String relativePath = indexPathStart + commaSeparatedResources + pathSuffix;
		CcpJsonRepresentation headers = this.headers;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, path);
		String bodyAsJson = body.asUgglyJson();
		V response = this.executeMeasured(trace, relativePath, path, method, http, headers, bodyAsJson, transformer);
		return response;
	}

	
	/**
	 * Runs a request routing the response by status through the given flows.
	 * @param <V> the result type
	 * @param trace identifier of the request in error messages
	 * @param url the path after the database URL
	 * @param method the HTTP method
	 * @param flows the flows by HTTP status
	 * @param body the request body
	 * @param transformer turns the response into the result
	 * @return the result
	 */
	public <V> V executeHttpRequest(String trace, String url, CcpHttpMethods method, CcpJsonRepresentation flows, CcpJsonRepresentation body, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();
		CcpJsonRepresentation headers = this.headers;
		String dbUrl = this.connectionDetails.getAsString(JsonFieldNames.DB_URL);
		String path = dbUrl + url;
		CcpHttpHandler http = new CcpHttpHandler(flows, path);
		String bodyAsJson = body.asUgglyJson();
		V response = this.executeMeasured(trace, url, path, method, http, headers, bodyAsJson, transformer);
		return response;
	}

	
	/**
	 * Runs a request accepting a single status.
	 * @param <V> the result type
	 * @param trace identifier of the request in error messages
	 * @param url the path after the database URL
	 * @param method the HTTP method
	 * @param expectedStatus the only accepted status
	 * @param body the request body
	 * @param transformer turns the response into the result
	 * @return the result
	 */
	public <V> V executeHttpRequest(String trace, String url, CcpHttpMethods method, Integer expectedStatus, CcpJsonRepresentation body, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();
		CcpJsonRepresentation headers = this.headers;
		String dbUrl = this.connectionDetails.getAsString(JsonFieldNames.DB_URL);
		String path = dbUrl + url;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, path);
		String bodyAsJson = body.asUgglyJson();
		V response = this.executeMeasured(trace, url, path, method, http, headers, bodyAsJson, transformer);
		return response;
	}

	
	/**
	 * Returns the connection details, loading them on the first call.
	 * @return the connection details
	 */
	public CcpJsonRepresentation getConnectionDetails() {
		this.loadConnectionProperties();
		return this.connectionDetails;
	}
	
	/**
	 * Recreates every entity of the folder (see {@link #executeDatabaseSetup}), writing the mapping errors to
	 * {@code mappingJnEntitiesErrors} and the results of the seed records to {@code insertResults}. A
	 * {@code ClassNotFoundException} is ignored; any other unexpected error aborts the setup.
	 * @param pathToCreateEntityScript the folder of the index scripts (one file per entity name)
	 * @param pathToJavaClasses the folder of the entity configurator sources
	 * @param mappingJnEntitiesErrors the file that receives the mapping errors
	 * @param insertResults the file that receives the results of the seed records
	 * @return this requester
	 */
	public CcpDbRequester createTables(String pathToCreateEntityScript, String pathToJavaClasses, String mappingJnEntitiesErrors, String insertResults) {

		String hostFolder = "java";
		CcpStringDecorator mappingErrorsPath = new CcpStringDecorator(mappingJnEntitiesErrors);
		CcpFileDecorator mappingErrorsFileDecorator = mappingErrorsPath.file();

		CcpFileDecorator mappingJnEntitiesErrorsFile = mappingErrorsFileDecorator.reset();

		CcpDbRequester database = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		Consumer<CcpErrorDbUtilsIncorrectEntityFields> whenIsIncorrectMapping = new ElasticSearchMappingErrorWriter(mappingJnEntitiesErrorsFile);

		Consumer<Throwable> whenOccursAnError = ElasticSearchSetupErrorHandler.INSTANCE;
		
		List<CcpBulkOperationResult> setupResults = database.executeDatabaseSetup(pathToJavaClasses, hostFolder,
				pathToCreateEntityScript, whenIsIncorrectMapping, whenOccursAnError);
				CcpStringDecorator insertResultsPath = new CcpStringDecorator(insertResults);
				CcpFileDecorator insertResultsFileDecorator = insertResultsPath.file();

				CcpFileDecorator createJnEntitiesFile = insertResultsFileDecorator.reset();
				String setupResultsAsText = setupResults.toString();
 	
				createJnEntitiesFile.write(setupResultsAsText);
		
		return this;
	}

	/**
	 * For each source file of the folder whose class is an entity configurator: checks that its fields match the
	 * {@code strict} mapping of its script, deletes and recreates its index (and the twin index) with the script, and
	 * collects its seed records; finally inserts every seed record in one bulk. The class name comes from the folder path
	 * after {@code hostFolder}.
	 * @param pathToJavaClasses the folder of the entity configurator sources
	 * @param hostFolder the folder that precedes the package path (e.g. "java")
	 * @param pathToCreateEntityScript the folder of the index scripts
	 * @param whenTheFieldsInTheEntityAreIncorrect receives the mapping errors
	 * @param whenOccursAnUnhadledError receives any other error
	 * @return the results of the seed records
	 */
	public List<CcpBulkOperationResult> executeDatabaseSetup(String pathToJavaClasses, String hostFolder, String pathToCreateEntityScript,	Consumer<CcpErrorDbUtilsIncorrectEntityFields> whenTheFieldsInTheEntityAreIncorrect,	Consumer<Throwable> whenOccursAnUnhadledError) {
		this.loadConnectionProperties();
		CcpHttpRequester http = CcpDependencyInjection.getDependency(CcpHttpRequester.class);
		CcpStringDecorator javaClassesPath = new CcpStringDecorator(pathToJavaClasses);
		CcpFolderDecorator folderJava = javaClassesPath.folder();
		List<CcpBulkItem> bulkItems = new ArrayList<>();
		ElasticSearchEntitySetupFromJavaFile entitySetup = new ElasticSearchEntitySetupFromJavaFile(this, http, pathToJavaClasses, hostFolder, pathToCreateEntityScript, bulkItems, whenTheFieldsInTheEntityAreIncorrect, whenOccursAnUnhadledError);
		folderJava.readFiles(entitySetup);
		CcpBulkExecutor bulk = CcpDependencyInjection.getDependency(CcpBulkExecutor.class);
		bulk = bulk.addRecords(bulkItems);
		List<CcpBulkOperationResult> bulkOperationResult = bulk.getBulkOperationResult();
		return bulkOperationResult;
	}


	/**
	 * Recreates the twin index of the entity, when it has one, with the same script.
	 * @param http the HTTP requester
	 * @param factory the entity factory
	 * @param scriptToCreateEntity the index script
	 * @param dbUrl the database URL
	 * @return this requester
	 */
	CcpDbRequester recreateEntityTwin(CcpHttpRequester http, CcpEntityFactory factory, String scriptToCreateEntity, String dbUrl) {
		
		CcpEntity entity = factory.entityInstance;
		
		boolean hasNoTwinEntity = false == factory.hasTwinEntity;
		
		if(hasNoTwinEntity) {
			return this;
		}
		CcpEntity twinEntity = entity.getTwinEntity();
		CcpEntityMetaData entityDetails = twinEntity.getEntityMetaData();
		String entityNameTwin = entityDetails.entityName;
		String dbUrlWithSlash = dbUrl + "/";
		String urlToEntityTwin = dbUrlWithSlash + entityNameTwin;
		this.recreateEntity(http, scriptToCreateEntity, urlToEntityTwin);
		return this;
	}


	/**
	 * Deletes the index (200 or 404 accepted) and creates it again with the script.
	 * @param http the HTTP requester
	 * @param scriptToCreateEntity the index script
	 * @param urlToEntity the URL of the index
	 * @return this requester
	 */
	CcpDbRequester recreateEntity(CcpHttpRequester http, String scriptToCreateEntity, String urlToEntity) {
		http.executeHttpRequest(urlToEntity, CcpHttpMethods.DELETE, this.headers, scriptToCreateEntity, 200, 404);
		http.executeHttpRequest(urlToEntity, CcpHttpMethods.PUT, this.headers, scriptToCreateEntity, 200);
		return this;
	}

	/**
	 * Creates the index with the script, leaving it as it is when it already exists (400 accepted).
	 * @param http the HTTP requester
	 * @param scriptToCreateEntity the index script
	 * @param urlToEntity the URL of the index
	 * @return this requester
	 */
	CcpDbRequester createEntityIfItDoesNotExist(CcpHttpRequester http, String scriptToCreateEntity, String urlToEntity) {
		http.executeHttpRequest(urlToEntity, CcpHttpMethods.PUT, this.headers, scriptToCreateEntity, 200, 400);
		return this;
	}

	/**
	 * Reads the index script, the file named after the entity in the scripts folder.
	 * @param pathToCreateEntityScript the scripts folder
	 * @param entityName the entity name
	 * @return the script
	 */
	String getScriptToCreateEntity(String pathToCreateEntityScript, String entityName) {
		String scriptFolderWithSlash = pathToCreateEntityScript + "/";
		String createEntityFile = scriptFolderWithSlash + entityName;
		CcpStringDecorator createEntityFilePath = new CcpStringDecorator(createEntityFile);
		CcpFileDecorator createEntityFileDecorator = createEntityFilePath.file();
		String scriptToCreateEntity = createEntityFileDecorator.getStringContent();
		return scriptToCreateEntity;
	}
	
	/**
	 * Checks that the mapping of the script is {@code dynamic: strict} and declares exactly the fields of the entity.
	 * @param entity the entity
	 * @param pathToCreateEntityScript the scripts folder
	 * @param className the configurator class name, for the message
	 * @return this requester
	 * @throws CcpErrorDbUtilsIncorrectEntityFields when the mapping is not strict or the fields differ
	 */
	CcpDbRequester validateEntityFields(CcpEntity entity, String pathToCreateEntityScript, String className) {
		
		CcpEntityMetaData entityDetails = entity.getEntityMetaData();
		String scriptToCreateEntity = this.getScriptToCreateEntity(pathToCreateEntityScript, entityDetails.entityName);
		CcpJsonRepresentation scriptToCreateEntityAsJson = new CcpJsonRepresentation(scriptToCreateEntity);
		CcpJsonRepresentation mappings = scriptToCreateEntityAsJson.getInnerJson(JsonFieldNames.mappings);
		String dynamic = mappings.getAsString(JsonFieldNames.dynamic);
		boolean isStrict = "strict".equals(dynamic);

		boolean isNotStrict = false == isStrict;
		
		if(isNotStrict) {
			// until 2026-10-07 the first placeholder got the value of dynamic, so the message did not name the entity
			String messageError = String.format("The entity '%s' has the dynamic property '%s' instead of 'strict'. The script to this entity is %s", entityDetails.entityName, dynamic, scriptToCreateEntityAsJson);
			CcpErrorDbUtilsIncorrectEntityFields ccpErrorDbUtilsIncorrectEntityFields = new CcpErrorDbUtilsIncorrectEntityFields(messageError);
			throw ccpErrorDbUtilsIncorrectEntityFields;
		}
		
		CcpJsonRepresentation propertiesJson = mappings.getInnerJson(JsonFieldNames.properties);
		Set<String> scriptFields = propertiesJson.fieldSet();
		CcpEntityField[] fields = entityDetails.allFields;
		Stream<CcpEntityField> entityFieldsStream = Arrays.asList(fields).stream();
		var fieldNamesStream = entityFieldsStream.map(x -> x.name());
		List<String> classFields = fieldNamesStream.collect(Collectors.toList());
		int scriptFieldsSize = scriptFields.size();
		Object[] scriptFieldsArray = scriptFields.toArray(new String[scriptFieldsSize]);
		CcpCollectionDecorator scriptFieldsCollection = new CcpCollectionDecorator(scriptFieldsArray);
		List<String> isInClassButIsNotInScript = scriptFieldsCollection.getExclusiveList(classFields);
		int classFieldsSize = classFields.size();
		Object[] classFieldsArray = classFields.toArray(new String[classFieldsSize]);
		CcpCollectionDecorator classFieldsCollection = new CcpCollectionDecorator(classFieldsArray);
		List<String> isInScriptButIsNotInClass = classFieldsCollection.getExclusiveList(scriptFields);
		String messageTemplateStart = "The class '%s'\n that belongs to the entity '%s'\n has an incorrect mapping, "
				+ "fields that are in script but are not in class %s,\n ";
				String messageTemplateWithClassFields = messageTemplateStart
				+ "fields that are in class but are not in script %s.\n ";
				String messageTemplate = messageTemplateWithClassFields
				+ "The script to this entity is %s";

				String messageError = String.format(messageTemplate, className, entityDetails.entityName, isInClassButIsNotInScript, 
				isInScriptButIsNotInClass, scriptToCreateEntityAsJson);
				boolean isInScriptButIsNotInClassEmpty = isInScriptButIsNotInClass.isEmpty();
				boolean hasFieldsMissingInClass = false == isInScriptButIsNotInClassEmpty;
		
		if(hasFieldsMissingInClass) {
			CcpErrorDbUtilsIncorrectEntityFields fieldsMissingInClassError = new CcpErrorDbUtilsIncorrectEntityFields(messageError);
			throw fieldsMissingInClassError;
		}
		boolean isInClassButIsNotInScriptEmpty = isInClassButIsNotInScript.isEmpty();

		boolean hasFieldsMissingInScript = false == isInClassButIsNotInScriptEmpty;

		if(hasFieldsMissingInScript) {
			CcpErrorDbUtilsIncorrectEntityFields fieldsMissingInScriptError = new CcpErrorDbUtilsIncorrectEntityFields(messageError);
			throw fieldsMissingInScriptError;
		}
		return this;
	}

	/**
	 * Backs up the indexes of the given names in a snapshot of the repository named after the bucket, registered first
	 * (the register is idempotent) with the kind of {@code elasticsearch.snapshot.repository}: Elasticsearch writes the
	 * files straight to the bucket. The snapshot is named {@code backup-<yyyyMMdd-HHmmss-SSS>}, keeps no state of the cluster,
	 * skips the names that match no index and is waited for.
	 * @param bucketName the bucket
	 * @param entityNamePatterns the names of the indexes, {@code *} standing for any text
	 * @return the {@code snapshot} of the response: its name, its indexes, its state and its shards
	 */
	public CcpJsonRepresentation backup(String bucketName, List<String> entityNamePatterns) {
		this.loadConnectionProperties();
		String snapshotPath = ElasticSearchDbRequesterSpecialWords.SNAPSHOT_PATH.getValue();
		String repositoryPath = snapshotPath + bucketName;
		CcpJsonRepresentation repository = this.snapshotRepositoryType.getRepository(bucketName);
		this.executeHttpRequest("registerSnapshotRepository", repositoryPath, CcpHttpMethods.PUT, 200, repository, CcpHttpResponseType.singleRecord);

		String snapshotNameFormat = ElasticSearchDbRequesterSpecialWords.SNAPSHOT_NAME_FORMAT.getValue();
		CcpTimeDecorator now = new CcpTimeDecorator();
		String snapshotName = now.getFormattedDateTime(snapshotNameFormat);
		String waitForCompletion = ElasticSearchDbRequesterSpecialWords.WAIT_FOR_COMPLETION.getValue();
		String snapshotOfTheRepository = repositoryPath + "/" + snapshotName;
		String snapshotUrl = snapshotOfTheRepository + waitForCompletion;
		String indices = String.join(",", entityNamePatterns);
		CcpJsonRepresentation bodyWithIndices = CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.indices, indices);
		CcpJsonRepresentation bodyWithIgnoreUnavailable = bodyWithIndices.put(JsonFieldNames.ignore_unavailable, true);
		CcpJsonRepresentation body = bodyWithIgnoreUnavailable.put(JsonFieldNames.include_global_state, false);
		CcpJsonRepresentation response = this.executeHttpRequest("createSnapshot", snapshotUrl, CcpHttpMethods.PUT, 200, body, CcpHttpResponseType.singleRecord);
		CcpJsonRepresentation snapshot = response.getInnerJson(JsonFieldNames.snapshot);
		return snapshot;
	}

	/**
	 * Refreshes the index of the entity. The request goes without a body: Elasticsearch 9 answers 400
	 * to a refresh that has one, even an empty JSON.
	 * @param entityName the name of the index
	 */
	public void refresh(String entityName) {
		String indexPathStart = ElasticSearchDbRequesterSpecialWords.INDEX_PATH_START.getValue();
		String refreshPath = ElasticSearchDbRequesterSpecialWords.REFRESH_PATH.getValue();
		String refreshUrl = indexPathStart + entityName + refreshPath;
		String refreshTrace = ElasticSearchDbRequesterSpecialWords.REFRESH_TRACE.getValue();
		String noBody = CcpOtherConstants.EMPTY_STRING.getValue();
		this.executeHttpRequest(refreshTrace, refreshUrl, CcpHttpMethods.POST, 200, noBody, CcpOtherConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
	}

	/**
	 * The index field of Elasticsearch documents.
	 * @return {@code _index}
	 */
	public String getFieldNameToEntity() {
		return "_index";
	}

	/**
	 * The id field of Elasticsearch documents.
	 * @return {@code _id}
	 */
	public String getFieldNameToId() {
		return "_id";
	}
}
