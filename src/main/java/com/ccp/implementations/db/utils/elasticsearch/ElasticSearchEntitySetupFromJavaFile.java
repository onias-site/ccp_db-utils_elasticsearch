package com.ccp.implementations.db.utils.elasticsearch;

import java.io.File;
import java.util.List;
import java.util.function.Consumer;

import com.ccp.decorators.CcpFileDecorator;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpReflectionConstructorDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.utils.entity.CcpEntity;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityFactory;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityMetaData;
import com.ccp.especifications.db.utils.entity.decorators.interfaces.CcpEntityConfigurator;
import com.ccp.especifications.db.utils.entity.fields.CcpErrorDbUtilsIncorrectEntityFields;
import com.ccp.especifications.http.CcpHttpRequester;
import com.ccp.implementations.db.utils.elasticsearch.ElasticSearchDbRequesterSpecialWords.JsonFieldNames;

/**
 * Step of {@code ElasticSearchDbRequester.executeDatabaseSetup} run over each source file of the folder: when its class
 * is an entity configurator, checks that its fields match the {@code strict} mapping of its script, deletes and
 * recreates its index (and the twin index) with the script, and collects its seed records. The class name comes from the
 * folder path after {@code hostFolder}.
 */
class ElasticSearchEntitySetupFromJavaFile implements Consumer<CcpFileDecorator> {

	/** The requester that recreates the indexes. */
	private final ElasticSearchDbRequester requester;

	/** The HTTP requester. */
	private final CcpHttpRequester http;

	/** The folder of the entity configurator sources. */
	private final String pathToJavaClasses;

	/** The folder that precedes the package path (e.g. "java"). */
	private final String hostFolder;

	/** The folder of the index scripts. */
	private final String pathToCreateEntityScript;

	/** Receives the seed records of every entity recreated. */
	private final List<CcpBulkItem> bulkItems;

	/** Receives the mapping errors. */
	private final Consumer<CcpErrorDbUtilsIncorrectEntityFields> whenTheFieldsInTheEntityAreIncorrect;

	/** Receives any other error. */
	private final Consumer<Throwable> whenOccursAnUnhadledError;

	/**
	 * Keeps what the setup of each entity needs.
	 * @param requester the requester that recreates the indexes
	 * @param http the HTTP requester
	 * @param pathToJavaClasses the folder of the entity configurator sources
	 * @param hostFolder the folder that precedes the package path
	 * @param pathToCreateEntityScript the folder of the index scripts
	 * @param bulkItems receives the seed records
	 * @param whenTheFieldsInTheEntityAreIncorrect receives the mapping errors
	 * @param whenOccursAnUnhadledError receives any other error
	 */
	ElasticSearchEntitySetupFromJavaFile(ElasticSearchDbRequester requester, CcpHttpRequester http, String pathToJavaClasses, String hostFolder, String pathToCreateEntityScript, List<CcpBulkItem> bulkItems, Consumer<CcpErrorDbUtilsIncorrectEntityFields> whenTheFieldsInTheEntityAreIncorrect, Consumer<Throwable> whenOccursAnUnhadledError) {
		this.requester = requester;
		this.http = http;
		this.pathToJavaClasses = pathToJavaClasses;
		this.hostFolder = hostFolder;
		this.pathToCreateEntityScript = pathToCreateEntityScript;
		this.bulkItems = bulkItems;
		this.whenTheFieldsInTheEntityAreIncorrect = whenTheFieldsInTheEntityAreIncorrect;
		this.whenOccursAnUnhadledError = whenOccursAnUnhadledError;
	}

	/**
	 * Recreates the entity of the source file, when its class is an entity configurator.
	 * @param javaFile the source file
	 */
	public void accept(CcpFileDecorator javaFile) {
		File file = new File(javaFile.content);
		String name = file.getName();
		String simpleClassName = name.replace(".java", "");
		String[] split = this.pathToJavaClasses.split(this.hostFolder);
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
			// as enums) that have no constructor without arguments and are not entities
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
			String scriptToCreateEntity = this.requester.getScriptToCreateEntity(this.pathToCreateEntityScript, entityDetails.entityName);

			this.requester.validateEntityFields(entity, this.pathToCreateEntityScript, className);

			CcpJsonRepresentation connectionDetails = this.requester.getConnectionDetails();
			String dbUrl = connectionDetails.getAsString(JsonFieldNames.DB_URL);
			String dbUrlWithSlash = dbUrl + "/";

			String urlToEntity = dbUrlWithSlash + entityDetails.entityName;
			this.requester.recreateEntity(this.http, scriptToCreateEntity, urlToEntity);
			this.requester.recreateEntityTwin(this.http, factory, scriptToCreateEntity, dbUrl);
			List<CcpBulkItem> firstRecordsToInsert = configurator.getFirstRecordsToInsert();
			this.bulkItems.addAll(firstRecordsToInsert);
		}catch(CcpErrorDbUtilsIncorrectEntityFields e) {
			this.whenTheFieldsInTheEntityAreIncorrect.accept(e);
		}catch (Throwable e) {
			this.whenOccursAnUnhadledError.accept(e);
		}
	}
}
