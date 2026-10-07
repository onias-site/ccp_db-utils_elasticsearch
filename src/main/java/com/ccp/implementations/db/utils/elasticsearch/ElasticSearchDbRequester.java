package com.ccp.implementations.db.utils.elasticsearch;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.ccp.constants.CcpOtherConstants;
import com.ccp.decorators.CcpCollectionDecorator;
import com.ccp.decorators.CcpFileDecorator;
import com.ccp.decorators.CcpFolderDecorator;
import com.ccp.decorators.CcpErrorInputStreamMissing;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpPropertiesDecorator;
import com.ccp.decorators.CcpReflectionConstructorDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpBulkExecutor;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.db.utils.entity.CcpEntity;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityFactory;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityMetaData;
import com.ccp.especifications.db.utils.entity.decorators.interfaces.CcpEntityConfigurator;
import com.ccp.especifications.db.utils.entity.fields.CcpEntityField;
import com.ccp.especifications.db.utils.entity.fields.CcpErrorDbUtilsIncorrectEntityFields;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.especifications.http.CcpHttpMethods;
import com.ccp.especifications.http.CcpHttpRequester;
import com.ccp.especifications.http.CcpHttpResponseTransform;
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
		headers = this.headers.mergeWithAnotherJson(headers);
		String dbUrl = this.connectionDetails.getAsString(JsonFieldNames.DB_URL);
		String path = dbUrl + url;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, path);
		V response = http.executeHttpRequest(trace, method, headers, body, transformer);
		return response;
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
		CcpJsonRepresentation headers = this.headers;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, path);
		V response = http.executeHttpRequest(trace, method, headers, body, transformer);
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
		V response = http.executeHttpRequest(trace, method, headers, body, transformer);
		
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
		V response = http.executeHttpRequest(trace, method, headers, body, transformer);
		
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
	 * {@code mappingJnEntitiesErrors} and the results of the seed records to {@code insertErrors}. A
	 * {@code ClassNotFoundException} is ignored; any other unexpected error aborts the setup.
	 * @param pathToCreateEntityScript the folder of the index scripts (one file per entity name)
	 * @param pathToJavaClasses the folder of the entity configurator sources
	 * @param mappingJnEntitiesErrors the file that receives the mapping errors
	 * @param insertErrors the file that receives the results of the seed records
	 * @return this requester
	 */
	public CcpDbRequester createTables(String pathToCreateEntityScript, String pathToJavaClasses, String mappingJnEntitiesErrors, String insertErrors) {

		String hostFolder = "java";
		CcpStringDecorator mappingErrorsPath = new CcpStringDecorator(mappingJnEntitiesErrors);
		CcpFileDecorator mappingErrorsFileDecorator = mappingErrorsPath.file();

		CcpFileDecorator mappingJnEntitiesErrorsFile = mappingErrorsFileDecorator.reset();

		CcpDbRequester database = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		Consumer<CcpErrorDbUtilsIncorrectEntityFields> whenIsIncorrectMapping = e -> {
			String message = e.getMessage();
			mappingJnEntitiesErrorsFile.append(message);
		};
		
		Consumer<Throwable> whenOccursAnError = e -> {
			boolean isClassNotFoundException = e instanceof ClassNotFoundException;

			if (isClassNotFoundException) {
				return;
			}
			CcpErrorElasticSearchDbSetupUnexpected ccpErrorElasticSearchDbSetupUnexpected = new CcpErrorElasticSearchDbSetupUnexpected(e);
			throw ccpErrorElasticSearchDbSetupUnexpected;
		};
		
		List<CcpBulkOperationResult> setupResults = database.executeDatabaseSetup(pathToJavaClasses, hostFolder,
				pathToCreateEntityScript, whenIsIncorrectMapping, whenOccursAnError);
				CcpStringDecorator insertErrorsPath = new CcpStringDecorator(insertErrors);
				CcpFileDecorator insertErrorsFileDecorator = insertErrorsPath.file();

				CcpFileDecorator createJnEntitiesFile = insertErrorsFileDecorator.reset();
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
		folderJava.readFiles(javaFile -> {
			File file = new File(javaFile.content);
			String name = file.getName();
			String simpleClassName = name.replace(".java", "");
			String[] split = pathToJavaClasses.split(hostFolder);
			int lastIndex = split.length - 1;
			String sourceFolder = split[lastIndex];
			String sourceFolderReplace = sourceFolder.replace("\\", ".");
			String packageName = sourceFolderReplace.replace("/", ".");
			boolean startsWithDot = packageName.startsWith(".");
			if(startsWithDot) {
				packageName = packageName.substring(1);
			}
			String packagePrefix = packageName + ".";
			String className = packagePrefix + simpleClassName;
			
			try {
				CcpStringDecorator classNameDecorator = new CcpStringDecorator(className);
			
				CcpReflectionConstructorDecorator reflection = classNameDecorator.reflection();
				boolean thisClassExists = reflection.thisClassExists();

				boolean thisClassDoesNotExist = false == thisClassExists;
				
				if(thisClassDoesNotExist) {
					return;
				}

				Class<?> clazz = reflection.forName();
				// checked on the class, before instantiating: the entities folder also holds auxiliary types (such
				// as VisMoneyTypes, an enum) that have no constructor without arguments and are not entities
				boolean isCcpEntityConfigurator = CcpEntityConfigurator.class.isAssignableFrom(clazz);

				boolean virtualEntity = false == isCcpEntityConfigurator;

				if(virtualEntity) {
					return;
				}

				Object newInstance = reflection.newInstance();

				CcpEntityConfigurator configurator = (CcpEntityConfigurator) newInstance;

				CcpEntityFactory factory = new CcpEntityFactory(clazz);

				CcpEntity entity = factory.entityInstance;
				
				CcpEntityMetaData entityDetails = entity.getEntityMetaData();
				String scriptToCreateEntity = this.getScriptToCreateEntity(pathToCreateEntityScript, entityDetails.entityName);
				
				this.validateEntityFields(entity, pathToCreateEntityScript, className);
				
				String dbUrl = this.connectionDetails.getAsString(JsonFieldNames.DB_URL);
				String dbUrlWithSlash = dbUrl + "/";

				String urlToEntity = dbUrlWithSlash + entityDetails.entityName;
				this.recreateEntity(http, scriptToCreateEntity, urlToEntity);
				this.recreateEntityTwin(http, factory, scriptToCreateEntity, dbUrl);
				List<CcpBulkItem> firstRecordsToInsert = configurator.getFirstRecordsToInsert();
				bulkItems.addAll(firstRecordsToInsert);
			}catch(CcpErrorDbUtilsIncorrectEntityFields e) {
				whenTheFieldsInTheEntityAreIncorrect.accept(e);
			}catch (Throwable e) {
				whenOccursAnUnhadledError.accept(e);
			}

		});	
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
	private CcpDbRequester recreateEntityTwin(CcpHttpRequester http, CcpEntityFactory factory, String scriptToCreateEntity, String dbUrl) {
		
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
	private CcpDbRequester recreateEntity(CcpHttpRequester http, String scriptToCreateEntity, String urlToEntity) {
		http.executeHttpRequest(urlToEntity, CcpHttpMethods.DELETE, this.headers, scriptToCreateEntity, 200, 404);
		http.executeHttpRequest(urlToEntity, CcpHttpMethods.PUT, this.headers, scriptToCreateEntity, 200);
		return this;
	}

	/**
	 * Reads the index script, the file named after the entity in the scripts folder.
	 * @param pathToCreateEntityScript the scripts folder
	 * @param entityName the entity name
	 * @return the script
	 */
	private String getScriptToCreateEntity(String pathToCreateEntityScript, String entityName) {
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
	private CcpDbRequester validateEntityFields(CcpEntity entity, String pathToCreateEntityScript, String className) {
		
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
