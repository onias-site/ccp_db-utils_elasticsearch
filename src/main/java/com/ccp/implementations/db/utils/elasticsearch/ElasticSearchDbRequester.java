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




/**
 * Implementação de {@code CcpDbRequester} para o Elasticsearch. Lê as propriedades de conexão
 * ({@code elasticsearch.address} / {@code elasticsearch.secret}) e executa requisições HTTP contra
 * o cluster. Também oferece {@code executeDatabaseSetup} para recriar índices e inserir registros
 * iniciais a partir de scripts de mapeamento.
 */
class ElasticSearchDbRequester implements CcpDbRequester {

	private CcpJsonRepresentation connectionDetails = CcpOtherConstants.EMPTY_JSON;
	
	private CcpDbRequester loadConnectionProperties() {
		boolean connectionDetailsEmpty = this.connectionDetails.isEmpty();
		boolean alreadyLoaded = false == connectionDetailsEmpty;
		if(alreadyLoaded) {
			return this;
		}
		CcpJsonRepresentation systemProperties;
		try {
			CcpStringDecorator ccpStringDecorator = new CcpStringDecorator("application_properties");
			CcpPropertiesDecorator propertiesFrom = ccpStringDecorator.propertiesFrom();
			systemProperties = propertiesFrom.environmentVariablesOrClassLoaderOrFile();
		} catch (CcpErrorInputStreamMissing e) {
			CcpJsonRepresentation put = CcpOtherConstants.EMPTY_JSON
					.put(ElasticSearchDbRequesterSpecialWords.elasticsearch_address, "http://localhost:9200");
					systemProperties = put
					.put(ElasticSearchDbRequesterSpecialWords.elasticsearch_secret, "")
					;
		}
		CcpJsonRepresentation putIfNotContains2 = systemProperties
				.putIfNotContains(ElasticSearchDbRequesterSpecialWords.elasticsearch_address, "http://localhost:9200");

				CcpJsonRepresentation putIfNotContains = putIfNotContains2
				.putIfNotContains(ElasticSearchDbRequesterSpecialWords.elasticsearch_secret, "");
				CcpJsonRepresentation jsonPiece = putIfNotContains.getJsonPiece(ElasticSearchDbRequesterSpecialWords.elasticsearch_address, ElasticSearchDbRequesterSpecialWords.elasticsearch_secret);
				CcpJsonRepresentation renameField = jsonPiece
				.renameField(ElasticSearchDbRequesterSpecialWords.elasticsearch_address, JsonFieldNames.DB_URL);

				CcpJsonRepresentation subMap = renameField.renameField(ElasticSearchDbRequesterSpecialWords.elasticsearch_secret, JsonFieldNames.Authorization)
				;
				CcpJsonRepresentation put2 = subMap
				.put(ElasticSearchDbRequesterSpecialWords.Content_Type, "application/json");

				this.connectionDetails = put2
				.put(JsonFieldNames.Accept, "application/json")
				;
		return this;
	}

	
	public <V> V executeHttpRequest(String trace, String url, CcpHttpMethods method,  Integer expectedStatus, String body, CcpJsonRepresentation headers, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();;
		headers = this.connectionDetails.mergeWithAnotherJson(headers);
		String asString = this.connectionDetails.getAsString(JsonFieldNames.DB_URL);
		String path = asString + url;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, path);
		V executeHttpRequest = http.executeHttpRequest(trace, method, headers, body, transformer);
		return executeHttpRequest;
	}

	
	public <V> V executeHttpRequest(String trace, String complemento, CcpHttpMethods method, Integer expectedStatus, CcpJsonRepresentation body,  String[] resources, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();
		String asString2 = this.connectionDetails.getAsString(JsonFieldNames.DB_URL);
		String asString2Mais = asString2 + "/";
		Stream<String> stream = Arrays.asList(resources).stream();
		List<String> collect = stream
				.collect(Collectors.toList());
				String toString = collect
				.toString();
				String toStringReplace = toString
				.replace("[", "");
				String toStringReplaceReplace = toStringReplace.replace("]", "");
				String toStringReplaceReplaceReplace = toStringReplaceReplace.replace(" ", "");
				String asString2MaisMais = asString2Mais +  toStringReplaceReplaceReplace;
				String path = asString2MaisMais + complemento;
		CcpJsonRepresentation headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, path);
		V executeHttpRequest = http.executeHttpRequest(trace, method, headers, body, transformer);
		return executeHttpRequest;
	}

	
	public <V> V executeHttpRequest(String trace, String url, CcpHttpMethods method, CcpJsonRepresentation flows, CcpJsonRepresentation body, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();
		CcpJsonRepresentation headers = this.connectionDetails;
		String asString3 = headers.getAsString(JsonFieldNames.DB_URL);
		String path = asString3 + url;
		CcpHttpHandler http = new CcpHttpHandler(flows, path);
		V executeHttpRequest = http.executeHttpRequest(trace, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	
	public <V> V executeHttpRequest(String trace, String url, CcpHttpMethods method, Integer expectedStatus, CcpJsonRepresentation body, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();
		CcpJsonRepresentation headers = this.connectionDetails;
		String asString4 = headers.getAsString(JsonFieldNames.DB_URL);
		String path = asString4 + url;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, path);
		V executeHttpRequest = http.executeHttpRequest(trace, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	
	public CcpJsonRepresentation getConnectionDetails() {
		this.loadConnectionProperties();
		return this.connectionDetails;
	}
	
	public CcpDbRequester createTables(String pathToCreateEntityScript, String pathToJavaClasses, String mappingJnEntitiesErrors, String insertErrors) {

		String hostFolder = "java";
		CcpStringDecorator ccpStringDecorator2 = new CcpStringDecorator(mappingJnEntitiesErrors);
		CcpFileDecorator ccpStringDecorator2File = ccpStringDecorator2.file();

		CcpFileDecorator mappingJnEntitiesErrorsFile = ccpStringDecorator2File.reset();

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
		
		List<CcpBulkOperationResult> executeDatabaseSetup = database.executeDatabaseSetup(pathToJavaClasses, hostFolder,
				pathToCreateEntityScript, whenIsIncorrectMapping, whenOccursAnError);
				CcpStringDecorator ccpStringDecorator3 = new CcpStringDecorator(insertErrors);
				CcpFileDecorator ccpStringDecorator3File = ccpStringDecorator3.file();

				CcpFileDecorator createJnEntitiesFile = ccpStringDecorator3File.reset();
				String toString2 = executeDatabaseSetup.toString();
 	
				createJnEntitiesFile.write(toString2);
		
		return this;
	}

	public List<CcpBulkOperationResult> executeDatabaseSetup(String pathToJavaClasses, String hostFolder, String pathToCreateEntityScript,	Consumer<CcpErrorDbUtilsIncorrectEntityFields> whenTheFieldsInTheEntityAreIncorrect,	Consumer<Throwable> whenOccursAnUnhadledError) {
		this.loadConnectionProperties();
		CcpHttpRequester http = CcpDependencyInjection.getDependency(CcpHttpRequester.class);
		CcpStringDecorator ccpStringDecorator4 = new CcpStringDecorator(pathToJavaClasses);
		CcpFolderDecorator folderJava = ccpStringDecorator4.folder();
		List<CcpBulkItem> bulkItems = new ArrayList<>();
		folderJava.readFiles(x -> {
			File file = new File(x.content);
			String name = file.getName();
			String replace = name.replace(".java", "");
			String[] split = pathToJavaClasses.split(hostFolder);
			int lengthMenos = split.length - 1;
			String sourceFolder = split[lengthMenos];
			String sourceFolderReplace = sourceFolder.replace("\\", ".");
			String packageName = sourceFolderReplace.replace("/", ".");
			boolean startsWith = packageName.startsWith(".");
			if(startsWith) {
				packageName = packageName.substring(1);
			}
			String packageNameMais = packageName + ".";
			String className = packageNameMais + replace;
			
			try {
				CcpStringDecorator ccpStringDecorator5 = new CcpStringDecorator(className);
			
				CcpReflectionConstructorDecorator reflection = ccpStringDecorator5.reflection();
				boolean thisClassExists = reflection.thisClassExists();

				boolean thisClassDoesNotExist = false == thisClassExists;
				
				if(thisClassDoesNotExist) {
					return;
				}

				Class<?> clazz = reflection.forName();
				Object newInstance = reflection.newInstance();
				boolean isCcpEntityConfigurator = newInstance instanceof CcpEntityConfigurator;

				boolean virtualEntity = false == isCcpEntityConfigurator;
				
				if(virtualEntity) {
					return;
				}
				
				CcpEntityConfigurator configurator = (CcpEntityConfigurator) newInstance;

				CcpEntityFactory factory = new CcpEntityFactory(clazz);

				CcpEntity entity = factory.entityInstance;
				
				CcpEntityMetaData entityDetails = entity.getEntityMetaData();
				String scriptToCreateEntity = this.getScriptToCreateEntity(pathToCreateEntityScript, entityDetails.entityName);
				
				this.validateEntityFields(entity, pathToCreateEntityScript, className);
				
				String dbUrl = this.connectionDetails.getAsString(JsonFieldNames.DB_URL);
				String dbUrlMais = dbUrl + "/";

				String urlToEntity = dbUrlMais + entityDetails.entityName;
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


	private CcpDbRequester recreateEntityTwin(CcpHttpRequester http, CcpEntityFactory factory, String scriptToCreateEntity, String dbUrl) {
		
		CcpEntity entity = factory.entityInstance;
		
		boolean hasNoTwinEntity = false == factory.hasTwinEntity;
		
		if(hasNoTwinEntity) {
			return this;
		}
		CcpEntity twinEntity = entity.getTwinEntity();
		CcpEntityMetaData entityDetails = twinEntity.getEntityMetaData();
		String entityNameTwin = entityDetails.entityName;
		String dbUrlMais2 = dbUrl + "/";
		String urlToEntityTwin = dbUrlMais2 + entityNameTwin;
		this.recreateEntity(http, scriptToCreateEntity, urlToEntityTwin);
		return this;
	}


	private CcpDbRequester recreateEntity(CcpHttpRequester http, String scriptToCreateEntity, String urlToEntity) {
		http.executeHttpRequest(urlToEntity, CcpHttpMethods.DELETE, this.connectionDetails, scriptToCreateEntity, 200, 404);
		http.executeHttpRequest(urlToEntity, CcpHttpMethods.PUT, this.connectionDetails, scriptToCreateEntity, 200);
		return this;
	}

	private String getScriptToCreateEntity(String pathToCreateEntityScript, String entityName) {
		String pathToCreateEntityScriptMais = pathToCreateEntityScript + "/";
		String createEntityFile = pathToCreateEntityScriptMais + entityName;
		CcpStringDecorator ccpStringDecorator6 = new CcpStringDecorator(createEntityFile);
		CcpFileDecorator ccpStringDecorator6File = ccpStringDecorator6.file();
		String scriptToCreateEntity = ccpStringDecorator6File.getStringContent();
		return scriptToCreateEntity;
	}
	
	private CcpDbRequester validateEntityFields(CcpEntity entity, String pathToCreateEntityScript, String className) {
		
		CcpEntityMetaData entityDetails = entity.getEntityMetaData();
		String scriptToCreateEntity = this.getScriptToCreateEntity(pathToCreateEntityScript, entityDetails.entityName);
		CcpJsonRepresentation scriptToCreateEntityAsJson = new CcpJsonRepresentation(scriptToCreateEntity);
		CcpJsonRepresentation mappings = scriptToCreateEntityAsJson.getInnerJson(JsonFieldNames.mappings);
		String dynamic = mappings.getAsString(JsonFieldNames.dynamic);
		boolean equals = "strict".equals(dynamic);

		boolean isNotStrict = false == equals;
		
		if(isNotStrict) {
			String messageError = String.format("The entity '%s' does not have the dynamic properties equals to strict. The script to this entity is %s", dynamic, scriptToCreateEntityAsJson);
			CcpErrorDbUtilsIncorrectEntityFields ccpErrorDbUtilsIncorrectEntityFields = new CcpErrorDbUtilsIncorrectEntityFields(messageError);
			throw ccpErrorDbUtilsIncorrectEntityFields;
		}
		
		CcpJsonRepresentation propertiesJson = mappings.getInnerJson(JsonFieldNames.properties);
		Set<String> scriptFields = propertiesJson.fieldSet();
		CcpEntityField[] fields = entityDetails.allFields;
		Stream<CcpEntityField> stream2 = Arrays.asList(fields).stream();
		var stream2Map = stream2.map(x -> x.name());
		List<String> classFields = stream2Map.collect(Collectors.toList());
		int scriptFieldsSize = scriptFields.size();
		Object[] array = scriptFields.toArray(new String[scriptFieldsSize]);
		CcpCollectionDecorator ccpCollectionDecorator = new CcpCollectionDecorator(array);
		List<String> isInClassButIsNotInScript = ccpCollectionDecorator.getExclusiveList(classFields);
		int classFieldsSize = classFields.size();
		Object[] array2 = classFields.toArray(new String[classFieldsSize]);
		CcpCollectionDecorator ccpCollectionDecorator2 = new CcpCollectionDecorator(array2);
		List<String> isInScriptButIsNotInClass = ccpCollectionDecorator2.getExclusiveList(scriptFields);
		String valorMais = "The class '%s'\n that belongs to the entity '%s'\n has an incorrect mapping, "
				+ "fields that are in script but are not in class %s,\n ";
				String valorMaisMais = valorMais
				+ "fields that are in class but are not in script %s.\n ";
				String valorMaisMaisMais = valorMaisMais
				+ "The script to this entity is %s";

				String messageError = String.format(valorMaisMaisMais, className, entityDetails.entityName, isInClassButIsNotInScript, 
				isInScriptButIsNotInClass, scriptToCreateEntityAsJson);
				boolean isInScriptButIsNotInClassEmpty = isInScriptButIsNotInClass.isEmpty();
				boolean missingsInClass = false == isInScriptButIsNotInClassEmpty;
		
		if(missingsInClass) {
			CcpErrorDbUtilsIncorrectEntityFields ccpErrorDbUtilsIncorrectEntityFields2 = new CcpErrorDbUtilsIncorrectEntityFields(messageError);
			throw ccpErrorDbUtilsIncorrectEntityFields2;
		}
		boolean isInClassButIsNotInScriptEmpty = isInClassButIsNotInScript.isEmpty();

		boolean missingsInScript = false == isInClassButIsNotInScriptEmpty;

		if(missingsInScript) {
			CcpErrorDbUtilsIncorrectEntityFields ccpErrorDbUtilsIncorrectEntityFields3 = new CcpErrorDbUtilsIncorrectEntityFields(messageError);
			throw ccpErrorDbUtilsIncorrectEntityFields3;
		}
		return this;
	}

	public String getFieldNameToEntity() {
		return "_index";
	}

	public String getFieldNameToId() {
		return "_id";
	}

	@SuppressWarnings("serial")
	private static class CcpErrorElasticSearchDbSetupUnexpected extends RuntimeException {
		private CcpErrorElasticSearchDbSetupUnexpected(Throwable cause) {
			super(cause);
		}
	}
}
