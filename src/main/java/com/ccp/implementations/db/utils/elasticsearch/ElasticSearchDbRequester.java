package com.ccp.implementations.db.utils.elasticsearch;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpCollectionDecorator;
import com.ccp.decorators.CcpErrorInputStreamMissing;
import com.ccp.decorators.CcpFileDecorator;
import com.ccp.decorators.CcpFolderDecorator;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
import com.ccp.decorators.CcpPropertiesDecorator;
import com.ccp.decorators.CcpReflectionConstructorDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;
import com.ccp.especifications.db.bulk.CcpDbBulkExecutor;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.db.utils.CcpEntity;
import com.ccp.especifications.db.utils.CcpEntityField;
import com.ccp.especifications.db.utils.CcpErrorDbUtilsIncorrectEntityFields;
import com.ccp.especifications.db.utils.decorators.engine.CcpEntityConfigurator;
import com.ccp.especifications.db.utils.decorators.engine.CcpEntityFactory;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.especifications.http.CcpHttpMethods;
import com.ccp.especifications.http.CcpHttpRequester;
import com.ccp.especifications.http.CcpHttpResponseTransform;
import com.ccp.implementations.db.utils.elasticsearch.ElasticSearchDbRequesterSpecialWords.JsonFieldNames;

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


class ElasticSearchDbRequester implements CcpDbRequester {

	private CcpJsonRepresentation connectionDetails = CcpOtherConstants.EMPTY_JSON;
	
	private CcpDbRequester loadConnectionProperties() {
		boolean alreadyLoaded = this.connectionDetails.isEmpty() == false;
		if(alreadyLoaded) {
			return this;
		}
		CcpJsonRepresentation systemProperties;
		try {
			CcpStringDecorator ccpStringDecorator = new CcpStringDecorator("application_properties");
			CcpPropertiesDecorator propertiesFrom = ccpStringDecorator.propertiesFrom();
			systemProperties = propertiesFrom.environmentVariablesOrClassLoaderOrFile();
		} catch (CcpErrorInputStreamMissing e) {
			systemProperties = CcpOtherConstants.EMPTY_JSON
					.put(ElasticSearchDbRequesterSpecialWords.elasticsearch_address, "http://localhost:9200")
					.put(ElasticSearchDbRequesterSpecialWords.elasticsearch_secret, "")
					;
		}
		
		CcpJsonRepresentation putIfNotContains = systemProperties
				.putIfNotContains(ElasticSearchDbRequesterSpecialWords.elasticsearch_address, "http://localhost:9200")
				.putIfNotContains(ElasticSearchDbRequesterSpecialWords.elasticsearch_secret, "");

		CcpJsonRepresentation subMap = putIfNotContains.getJsonPiece(ElasticSearchDbRequesterSpecialWords.elasticsearch_address, ElasticSearchDbRequesterSpecialWords.elasticsearch_secret)
				.renameField(ElasticSearchDbRequesterSpecialWords.elasticsearch_address, JsonFieldNames.DB_URL).renameField(ElasticSearchDbRequesterSpecialWords.elasticsearch_secret, JsonFieldNames.Authorization)
				;
		
		this.connectionDetails = subMap
				.put(ElasticSearchDbRequesterSpecialWords.Content_Type, "application/json")
				.put(JsonFieldNames.Accept, "application/json")
				;
		return this;
	}

	
	public <V> V executeHttpRequest(String trace, String url, CcpHttpMethods method,  Integer expectedStatus, String body, CcpJsonRepresentation headers, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();;
		headers = this.connectionDetails.putAll(headers);
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus);
		String path = this.connectionDetails.getAsString(JsonFieldNames.DB_URL) + url;
		V executeHttpRequest = http.executeHttpRequest(trace, path, method, headers, body, transformer);
		return executeHttpRequest;
	}

	
	public <V> V executeHttpRequest(String trace, String complemento, CcpHttpMethods method, Integer expectedStatus, CcpJsonRepresentation body,  String[] resources, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();
		String path = this.connectionDetails.getAsString(JsonFieldNames.DB_URL) + "/" +  Arrays.asList(resources).stream()
				.collect(Collectors.toList())
				.toString()
				.replace("[", "").replace("]", "").replace(" ", "") + complemento;
		CcpJsonRepresentation headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus);
		V executeHttpRequest = http.executeHttpRequest(trace, path, method, headers, body, transformer);
		return executeHttpRequest;
	}

	
	public <V> V executeHttpRequest(String trace, String url, CcpHttpMethods method, CcpJsonRepresentation flows, CcpJsonRepresentation body, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();
		CcpJsonRepresentation headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(flows);
		String path = headers.getAsString(JsonFieldNames.DB_URL) + url;
		V executeHttpRequest = http.executeHttpRequest(trace, path, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	
	public <V> V executeHttpRequest(String trace, String url, CcpHttpMethods method, Integer expectedStatus, CcpJsonRepresentation body, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();
		CcpJsonRepresentation headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus);
		String path = headers.getAsString(JsonFieldNames.DB_URL) + url;
		V executeHttpRequest = http.executeHttpRequest(trace, path, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	
	public CcpJsonRepresentation getConnectionDetails() {
		this.loadConnectionProperties();
		return this.connectionDetails;
	}
	
	public CcpDbRequester createTables(String pathToCreateEntityScript, String pathToJavaClasses, String mappingJnEntitiesErrors, String insertErrors) {

		String hostFolder = "java";

		CcpFileDecorator mappingJnEntitiesErrorsFile = new CcpStringDecorator(mappingJnEntitiesErrors).file().reset();

		CcpDbRequester database = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		Consumer<CcpErrorDbUtilsIncorrectEntityFields> whenIsIncorrectMapping = e -> {
			String message = e.getMessage();
			mappingJnEntitiesErrorsFile.append(message);
		};
		
		Consumer<Throwable> whenOccursAnError = e -> {

			if (e instanceof ClassNotFoundException) {
				return;
			}
			throw new RuntimeException(e);
		};
		
		List<CcpBulkOperationResult> executeDatabaseSetup = database.executeDatabaseSetup(pathToJavaClasses, hostFolder,
				pathToCreateEntityScript, whenIsIncorrectMapping, whenOccursAnError);

		CcpFileDecorator createJnEntitiesFile = new CcpStringDecorator(insertErrors).file().reset();
		 
		createJnEntitiesFile.write(executeDatabaseSetup.toString());
		
		return this;
	}

	public List<CcpBulkOperationResult> executeDatabaseSetup(String pathToJavaClasses, String hostFolder, String pathToCreateEntityScript,	Consumer<CcpErrorDbUtilsIncorrectEntityFields> whenTheFieldsInTheEntityAreIncorrect,	Consumer<Throwable> whenOccursAnUnhadledError) {
		this.loadConnectionProperties();
		CcpHttpRequester http = CcpDependencyInjection.getDependency(CcpHttpRequester.class);
		CcpFolderDecorator folderJava = new CcpStringDecorator(pathToJavaClasses).folder();
		List<CcpBulkItem> bulkItems = new ArrayList<>();
		folderJava.readFiles(x -> {
			String name = new File(x.content).getName();
			String replace = name.replace(".java", "");
			String[] split = pathToJavaClasses.split(hostFolder);
			String sourceFolder = split[split.length - 1];
			String packageName = sourceFolder.replace("\\", ".").replace("/", ".");
			if(packageName.startsWith(".")) {
				packageName = packageName.substring(1);
			}
			String className = packageName + "." + replace;
			
			try {
				
				CcpReflectionConstructorDecorator reflection = new CcpStringDecorator(className).reflection();
				
				boolean thisClassDoesNotExist = reflection.thisClassExists() == false;
				
				if(thisClassDoesNotExist) {
					return;
				}

				Class<?> clazz = reflection.forName();
				Object newInstance = reflection.newInstance();
				
				boolean virtualEntity = newInstance instanceof CcpEntityConfigurator == false;
				
				if(virtualEntity) {
					return;
				}
				CcpEntityConfigurator configurator = (CcpEntityConfigurator) newInstance;

				CcpEntityFactory factory = new CcpEntityFactory(clazz);

				CcpEntity entity = factory.entityInstance;
				
				String entityName = entity.getEntityName();
				String scriptToCreateEntity = this.getScriptToCreateEntity(pathToCreateEntityScript, entityName);
				
				this.validateEntityFields(entity, pathToCreateEntityScript, className);
				
				String dbUrl = this.connectionDetails.getAsString(JsonFieldNames.DB_URL);
				
				String urlToEntity = dbUrl + "/" + entityName;
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
		CcpDbBulkExecutor bulk = CcpDependencyInjection.getDependency(CcpDbBulkExecutor.class);
		bulk = bulk.addRecords(bulkItems);
		List<CcpBulkOperationResult> bulkOperationResult = bulk.getBulkOperationResult();
		return bulkOperationResult;
	}


	private CcpDbRequester recreateEntityTwin(CcpHttpRequester http, CcpEntityFactory factory, String scriptToCreateEntity, String dbUrl) {
		
		CcpEntity entity = factory.entityInstance;
		
		boolean hasNoTwinEntity = factory.hasTwinEntity == false;
		
		if(hasNoTwinEntity) {
			return this;
		}
		CcpEntity twinEntity = entity.getTwinEntity();
		String entityNameTwin = twinEntity.getEntityName();
		String urlToEntityTwin = dbUrl + "/" + entityNameTwin;
		this.recreateEntity(http, scriptToCreateEntity, urlToEntityTwin);
		return this;
	}


	private CcpDbRequester recreateEntity(CcpHttpRequester http, String scriptToCreateEntity, String urlToEntity) {
		http.executeHttpRequest(urlToEntity, CcpHttpMethods.DELETE, this.connectionDetails, scriptToCreateEntity, 200, 404);
		http.executeHttpRequest(urlToEntity, CcpHttpMethods.PUT, this.connectionDetails, scriptToCreateEntity, 200);
		return this;
	}

	private String getScriptToCreateEntity(String pathToCreateEntityScript, String entityName) {
		String createEntityFile = pathToCreateEntityScript + "/" + entityName;
		String scriptToCreateEntity = new CcpStringDecorator(createEntityFile).file().getStringContent();
		return scriptToCreateEntity;
	}
	
	private CcpDbRequester validateEntityFields(CcpEntity entity, String pathToCreateEntityScript, String className) {
		
		String entityName = entity.getEntityName();
		String scriptToCreateEntity = this.getScriptToCreateEntity(pathToCreateEntityScript, entityName);
		CcpJsonRepresentation scriptToCreateEntityAsJson = new CcpJsonRepresentation(scriptToCreateEntity);
		CcpJsonRepresentation mappings = scriptToCreateEntityAsJson.getInnerJson(JsonFieldNames.mappings);
		String dynamic = mappings.getAsString(JsonFieldNames.dynamic);
		
		boolean isNotStrict = "strict".equals(dynamic) == false;
		
		if(isNotStrict) {
			String messageError = String.format("The entity '%s' does not have the dynamic properties equals to strict. The script to this entity is %s", dynamic, scriptToCreateEntityAsJson);
			throw new CcpErrorDbUtilsIncorrectEntityFields(messageError);
		}
		
		CcpJsonRepresentation propertiesJson = mappings.getInnerJson(JsonFieldNames.properties);
		Set<String> scriptFields = propertiesJson.fieldSet();
		CcpEntityField[] fields = entity.getFields();
		List<String> classFields = Arrays.asList(fields).stream().map(x -> x.name()).collect(Collectors.toList());
		Object[] array = scriptFields.toArray(new String[scriptFields.size()]);
		List<String> isInClassButIsNotInScript = new CcpCollectionDecorator(array).getExclusiveList(classFields);
		Object[] array2 = classFields.toArray(new String[classFields.size()]);
		List<String> isInScriptButIsNotInClass = new CcpCollectionDecorator(array2).getExclusiveList(scriptFields);
		
		String messageError = String.format("The class '%s'\n that belongs to the entity '%s'\n has an incorrect mapping, "
				+ "fields that are in script but are not in class %s,\n "
				+ "fields that are in class but are not in script %s.\n "
				+ "The script to this entity is %s", className, entityName, isInClassButIsNotInScript, 
				isInScriptButIsNotInClass, scriptToCreateEntityAsJson);
		boolean missingsInClass = isInScriptButIsNotInClass.isEmpty() == false;
		
		if(missingsInClass) {
			throw new CcpErrorDbUtilsIncorrectEntityFields(messageError);
		}
		
		boolean missingsInScript = isInClassButIsNotInScript.isEmpty() == false;

		if(missingsInScript) {
			throw new CcpErrorDbUtilsIncorrectEntityFields(messageError);
		}
		return this;
	}

	public String getFieldNameToEntity() {
		return "_index";
	}

	public String getFieldNameToId() {
		return "_id";
	}
}
