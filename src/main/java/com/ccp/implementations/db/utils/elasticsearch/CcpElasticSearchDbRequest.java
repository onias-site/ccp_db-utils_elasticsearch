package com.ccp.implementations.db.utils.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.db.utils.CcpDbRequester;

public class CcpElasticSearchDbRequest implements CcpInstanceProvider<CcpDbRequester> {

	public CcpDbRequester getInstance() {
		return new ElasticSearchDbRequester();
	}

}
