/*******************************************************************************
 * Copyright (c) 2019 Eclipse RDF4J contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package org.eclipse.rdf4j.federated;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.federated.endpoint.Endpoint;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.http.client.HttpClientSessionManager;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.federation.RepositoryFederatedService;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLFederatedService;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

public class ServiceTests extends SPARQLBaseTest {

	@Test
	public void test1() throws Exception {
		System.out.println("test1: starting");
		assumeSparqlEndpoint();
		System.out.println("test1: assumeSparqlEndpoint(...) done");
		/* test select query retrieving all persons from endpoint 1 (SERVICE) */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test1: prepareTest(...) done");
		evaluateQueryPlan("/tests/service/query01.rq", "/tests/service/query01.qp");
		System.out.println("test1: evaluateQueryPlan(...) done");
		execute("/tests/service/query01.rq", "/tests/service/query01.srx", false, true);
		System.out.println("test1: execute(...) done");
	}

	@Test
	public void test1a_byName() throws Exception {
		System.out.println("test1a_byName: starting");
		/* test select query retrieving all persons from endpoint 1 (SERVICE) by name */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test1a_byName: prepareTest(...) done");
		evaluateQueryPlan("/tests/service/query01a.rq", "/tests/service/query01.qp");
		System.out.println("test1a_byName: evaluateQueryPlan(...)");
		execute("/tests/service/query01a.rq", "/tests/service/query01.srx", false, true);
		System.out.println("test1a_Name: execute(...) done");
	}

	@Test
	public void test2() throws Exception {
		System.out.println("test2: starting");
		assumeSparqlEndpoint();
		System.out.println("test2: assumeSparqlEndpoint() done");
		/* test select query retrieving all persons from endpoint 1 (SERVICE) + exclusive statement => group */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test2: prepareTest(...) done");
		evaluateQueryPlan("/tests/service/query02.rq", "/tests/service/query02.qp");
		System.out.println("test2: evaluateQueryPlan(...)");
		execute("/tests/service/query02.rq", "/tests/service/query02.srx", false, true);
		System.out.println("test2: execute(...) done");
	}

	@Test
	public void test2_differentOrder() throws Exception {
		System.out.println("test2_differentOrder: starting");
		assumeSparqlEndpoint();
		System.out.println("test2_differentOrder: assumeSparqlEndpoint() done");
		/*
		 * test select query retrieving all persons from endpoint 1 (SERVICE) + exclusive statement => group In contrast
		 * to test2: order is different
		 */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test2_differentOrder: prepareTest(...) done");
		evaluateQueryPlan("/tests/service/query02a.rq", "/tests/service/query02a.qp");
		System.out.println("test2_differentOrder: evaluateQueryPlan(...) done");
		execute("/tests/service/query02a.rq", "/tests/service/query02.srx", false, true);
		System.out.println("test2_differentOrder: execute(...) done");
	}

	@Test
	public void test3() throws Exception {
		System.out.println("test3: starting");
		assumeSparqlEndpoint();
		System.out.println("test3: assumeSparqlEndpoint() done");
		/*
		 * test select query retrieving all persons from endpoint 1 (SERVICE), endpoint not part of federation =>
		 * evaluate using RDF4J
		 */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test3: prepareTest(...) done");
		Endpoint endpoint1 = federationContext().getEndpointManager().getEndpointByName("http://endpoint1");
		System.out.println("test3: endpoint1 created");
		fedxRule.removeEndpoint(endpoint1);
		System.out.println("test3: fedxRule.removeEndpoint(endpoint1) done");
		execute("/tests/service/query03.rq", "/tests/service/query03.srx", false, true);
		System.out.println("test3: execute(...) done");
	}

	@Test
	public void test4() throws Exception {
		System.out.println("test4: starting");
		// evaluates by sparql endpoint URL, cannot be done with native store
		assumeSparqlEndpoint();
		System.out.println("test4: assumeSparqlEndpoint() done");
		/* two service which form exclusive groups */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test4: prepareTest(...) done");
		evaluateQueryPlan("/tests/service/query04.rq", "/tests/service/query04.qp");
		System.out.println("test4: evaluateQueryPlan(...) done");
		execute("/tests/service/query04.rq", "/tests/service/query04.srx", false, true);
		System.out.println("test4: execute(...) done");
	}

	@Test
	public void test4a() throws Exception {
		System.out.println("test4a: starting");
		/* two service which form exclusive groups (resolving by name) */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test4a: prepareTest(...) done");
		evaluateQueryPlan("/tests/service/query04a.rq", "/tests/service/query04.qp");
		System.out.println("test4a: evaluateQueryPlan(...) done");
		execute("/tests/service/query04a.rq", "/tests/service/query04a.srx", false, true);
		System.out.println("test4a: execute(...) done");
	}

	@Test
	public void test5() throws Exception {
		System.out.println("test5: starting");
		assumeSparqlEndpoint();
		System.out.println("test5: assumeSparqlEndpoint() done");
		/* two services, one becomes exclusive group, the other is evaluated as service (filter) */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test5: prepareTest(...) done");
		execute("/tests/service/query05.rq", "/tests/service/query05.srx", false, true);
		System.out.println("test5: execute(...) done");
	}

	@Test
	public void test6() throws Exception {
		System.out.println("test6: starting");
		/*
		 * two services, one becomes exclusive group, the other is evaluated as service (filter), uses name of
		 * federation member in SERVICE
		 */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test6: prepareTest(...) done");
		execute("/tests/service/query06.rq", "/tests/service/query06.srx", false, true);
		System.out.println("test6: execute(...) done");
	}

	@Test
	public void test7() throws Exception {
		System.out.println("test7: starting");
		// evaluates by sparql endpoint URL, cannot be done with native store
		assumeSparqlEndpoint();
		System.out.println("test7: assumeSparqlEndpoint() done");
		/* two services, both evaluated as SERVICE (FILTER), uses name of federation member in SERVICE */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test7: prepareTest(...) done");
		execute("/tests/service/query07.rq", "/tests/service/query07.srx", false, true);
		System.out.println("test7: execute(...) done");
	}

	@Test
	public void test8() throws Exception {
		System.out.println("test8: starting");
		assumeSparqlEndpoint();
		System.out.println("test8: assumeSparqlEndpoint() done");
		/*
		 * test select query retrieving all persons from endpoint 1 (SERVICE) + exclusive statement => group
		 */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test8: prepareTest(...) done");
		evaluateQueryPlan("/tests/service/query08.rq", "/tests/service/query08.qp");
		System.out.println("test8: evaluateQueryPlan(...) done");
		execute("/tests/service/query08.rq", "/tests/service/query08.srx", false, true);
		System.out.println("test8: execute(...) done");
	}

	@Test
	public void test9() throws Exception {
		System.out.println("test9: starting");
		assumeSparqlEndpoint();
		System.out.println("test9: assumeSparqlEndpoint() done");
		FederatedServiceResolver serviceResolver = new SPARQLServiceResolver() {
			@Override
			protected FederatedService createService(String serviceUrl) throws QueryEvaluationException {
				System.out.println("test9: createService() called");
				return new TestSparqlFederatedService(serviceUrl, getHttpClientSessionManager());
			}
		};
		System.out.println("test9: serviceResolver created");
		// workaround for test: shutdown and re-initialize in order to set a custom federated service
		FedXRepository repo = fedxRule.getRepository();
		System.out.println("test9: repo created");
		repo.shutDown();
		System.out.println("test9: repo.shutDown() done");
		repo.setFederatedServiceResolver(serviceResolver);
		System.out.println("test9: repo.setFederatedServiceResolver(serviceResolver) done");
		repo.init();
		System.out.println("test9: repo.init() done");

		/*
		 * test select query retrieving all persons from endpoint 1 (SERVICE), endpoint not part of federation =>
		 * evaluate using externally provided service resolver endpoint1 is reachable as
		 * http://localhost:18080/repositories/endpoint1 via HTTP
		 */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test9: prepareTest(...) done");
		Endpoint endpoint1 = federationContext().getEndpointManager().getEndpointByName("http://endpoint1");
		System.out.println("test9: endPoint1 created");
		fedxRule.removeEndpoint(endpoint1);
		System.out.println("test9: fedxRule.removeEndpoint(endpoint1) done");
		execute("/tests/service/query03.rq", "/tests/service/query03.srx", false, false);
		System.out.println("test9: execute(...) done");
		Assertions.assertEquals(1,
				((TestSparqlFederatedService) serviceResolver
						.getService("http://localhost:18080/repositories/endpoint1")).serviceRequestCount.get());
	}

	@Test
	public void test10_serviceBoundJoin() throws Exception {
		System.out.println("test10_serviceBoundJoin: starting");
		assumeSparqlEndpoint();
		System.out.println("test10_serviceBoundJoin: assumeSparqlEndpoint() done");
		FederatedServiceResolver serviceResolver = new SPARQLServiceResolver() {
			@Override
			protected FederatedService createService(String serviceUrl) throws QueryEvaluationException {
				System.out.println("test10_serviceBoundJoin: createService() called");
				return new TestSparqlFederatedService(serviceUrl, getHttpClientSessionManager());
			}
		};
		System.out.println("test10_serviceBoundJoin: serviceResolver created");
		// workaround for test: shutdown and re-initialize in order to set a custom federated service
		FedXRepository repo = fedxRule.getRepository();
		System.out.println("test10_serviceBoundJoin: repo created");
		repo.shutDown();
		System.out.println("test10_serviceBoundJoin: repo.shutDown() done");
		repo.setFederatedServiceResolver(serviceResolver);
		System.out.println("test10_serviceBoundJoin: repo.setFederatedServiceResolver(serviceResolver) done");
		repo.init();
		System.out.println("test10_serviceBoundJoin: repo.init() done");
		fedxRule.getFederationContext().getConfig().withBoundJoinBlockSize(5);
		System.out.println(
				"test10_serviceBoundJoin: fedxRule.getFederationContext().getConfig().withBoundJoinBlockSize(5) done");
		/*
		 * test select query retrieving all persons from endpoint 1 (SERVICE), endpoint not part of federation =>
		 * evaluate using externally provided service resolver endpoint1 is reachable as
		 * http://localhost:18080/repositories/endpoint1 via HTTP
		 */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test10_serviceBoundJoin: prepareTest(...) done");
		Endpoint endpoint1 = federationContext().getEndpointManager().getEndpointByName("http://endpoint1");
		System.out.println("test10_serviceBoundJoin: endpoint1 created");
		fedxRule.removeEndpoint(endpoint1);
		System.out.println("test10_serviceBoundJoin: fedxRule.removeEndpoint(endpoint1) done");

		StringBuilder query = new StringBuilder();
		query.append("SELECT * WHERE { VALUES ?input { ");
		for (int i = 0; i < 50; i++) {
			query.append(" \"input").append(i).append("\" ");
		}
		query.append(" }");
		query.append(
				" SERVICE <http://localhost:18080/repositories/endpoint1> { BIND (CONCAT(?input, '_processed') AS ?output) } ");
		query.append(" }");
		System.out.println("test10_serviceBoundJoin: query created/populated");

		try (TupleQueryResult tqr = queryManager().prepareTupleQuery(query.toString()).evaluate()) {
			List<BindingSet> res = Iterations.asList(tqr);
			Assertions.assertEquals(50, res.size());
			Set<Value> expected = Sets.newHashSet();
			System.out.println("test10_serviceBoundJoin: expected created");
			for (int i = 0; i < 50; i++) {
				expected.add(SimpleValueFactory.getInstance().createLiteral("input" + i + "_processed"));
			}
			System.out.println("test10_serviceBoundJoin: expected populated");
			Assertions.assertEquals(expected, res.stream().map(b -> b.getValue("output")).collect(Collectors.toSet()));
		}
		System.out.println("test10_serviceBoundJoin: try block done");
		// all requests are executed in bind-join with constant size
		// for this test bind join size is set to 5, hence we see 10 bind join requests
		TestSparqlFederatedService tfs = ((TestSparqlFederatedService) serviceResolver
				.getService("http://localhost:18080/repositories/endpoint1"));
		System.out.println("test10_serviceBoundJoin: tfs created");
		Assertions.assertEquals(10, tfs.boundJoinRequestCount.get());
	}

	@Test
	public void test10_serviceSimpleEvaluation() throws Exception {
		System.out.println("test10_serviceSimpleEvaluation: starting");
		assumeSparqlEndpoint();
		System.out.println("test10_serviceSimpleEvaluation: assumeSparqlEndpoint() done");
		fedxRule.setConfig(c -> c.withEnableServiceAsBoundJoin(false));
		System.out.println("test10_serviceSimpleEvaluation: L332");
		FederatedServiceResolver serviceResolver = new SPARQLServiceResolver() {
			@Override
			protected FederatedService createService(String serviceUrl) throws QueryEvaluationException {
				System.out.println("test10_serviceSimpleEvaluation: createService() called");
				return new TestSparqlFederatedService(serviceUrl, getHttpClientSessionManager());
			}
		};
		System.out.println("test10_serviceSimpleEvaluation: serviceResolver() created");
		// workaround for test: shutdown and re-initialize in order to set a custom federated service
		FedXRepository repo = fedxRule.getRepository();
		System.out.println("test10_serviceSimpleEvaluation: repo created");
		repo.shutDown();
		System.out.println("test10_serviceSimpleEvaluation: repo.shutDown() done");
		repo.setFederatedServiceResolver(serviceResolver);
		System.out.println("test10_serviceSimpleEvaluation: repo.setFederatedServiceResolver(serviceResolver) done");
		repo.init();
		System.out.println("test10_serviceSimpleEvaluation: repo.init() done");

		/*
		 * test select query retrieving all persons from endpoint 1 (SERVICE), endpoint not part of federation =>
		 * evaluate using externally provided service resolver endpoint1 is reachable as
		 * http://localhost:18080/repositories/endpoint1 via HTTP
		 */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test10_serviceSimpleEvaluation: prepareTest(...) done");
		Endpoint endpoint1 = federationContext().getEndpointManager().getEndpointByName("http://endpoint1");
		System.out.println("test10_serviceSimpleEvaluation: endpoint1 created");
		fedxRule.removeEndpoint(endpoint1);
		System.out.println("test10_serviceSimpleEvaluation: fedxRule.removeEndpoint(endpoint1) done");
		StringBuilder query = new StringBuilder();
		query.append("SELECT * WHERE { VALUES ?input { ");
		for (int i = 0; i < 50; i++) {
			query.append(" \"input").append(i).append("\" ");
		}
		query.append(" }");
		query.append(
				" SERVICE <http://localhost:18080/repositories/endpoint1> { BIND (CONCAT(?input, '_processed') AS ?output) } ");
		query.append(" }");
		System.out.println("test10_serviceSimpleEvaluation: query created/populated");
		try (TupleQueryResult tqr = queryManager().prepareTupleQuery(query.toString()).evaluate()) {
			List<BindingSet> res = Iterations.asList(tqr);
			Assertions.assertEquals(50, res.size());
			Set<Value> expected = Sets.newHashSet();
			for (int i = 0; i < 50; i++) {
				expected.add(SimpleValueFactory.getInstance().createLiteral("input" + i + "_processed"));
			}
			System.out.println("test10_serviceSimpleEvaluation: expected populated");
			Assertions.assertEquals(expected, res.stream().map(b -> b.getValue("output")).collect(Collectors.toSet()));
		}
		System.out.println("test10_serviceSimpleEvaluation: try block done");
		// all input bindings are evaluated as simple join
		TestSparqlFederatedService tfs = ((TestSparqlFederatedService) serviceResolver
				.getService("http://localhost:18080/repositories/endpoint1"));
		System.out.println("test10_serviceSimpleEvaluation: tfs created");
		Assertions.assertEquals(50, tfs.serviceRequestCount.get());
		Assertions.assertEquals(0, tfs.boundJoinRequestCount.get());
	}

	@Test
	public void test10_serviceSilent() throws Exception {
		System.out.println("test10_serviceSilent: starting");
		assumeSparqlEndpoint();
		System.out.println("test10_serviceSilent: assumeSparqlEndpoint()");
		Repository localStore = new SailRepository(new MemoryStore());
		System.out.println("test10_serviceSilent: localStore created");
		SPARQLServiceResolver serviceResolver = new SPARQLServiceResolver() {
			@Override
			protected FederatedService createService(String serviceUrl) throws QueryEvaluationException {
				System.out.print("test10_serviceSilent: createService called - ");
				if (serviceUrl.equals("urn:memStore")) {
					System.out.println("if branch");
					return new RepositoryFederatedService(localStore, true);
				}
				System.out.println("else branch");
				return new TestSparqlFederatedService(serviceUrl, getHttpClientSessionManager());
			}
		};
		System.out.println("test10_serviceSilent: serviceResolver created");
		// workaround for test: shutdown and re-initialize in order to set a custom service resolver
		FedXRepository repo = fedxRule.getRepository();
		System.out.println("test10_serviceSilent: repo created");
		repo.shutDown();
		System.out.println("test10_serviceSilent: repo.shutDown() done");
		repo.setFederatedServiceResolver(serviceResolver);
		System.out.println("test10_serviceSilent: repo.setFederatedServiceResolver(serviceResolver) done");
		repo.init();
		System.out.println("test10_serviceSilent: repo.init() done");

		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		System.out.println("test10_serviceSilent: prepareTest(...) done");
		List<BindingSet> bs = Repositories.tupleQueryNoTransaction(fedxRule.repository,
				"SELECT * WHERE { VALUES ?input { 'input1'  } . SERVICE SILENT <urn:memStore> { BIND (CONCAT(?input, '_processed') AS ?output) } }",
				iter -> QueryResults.asList(iter));
		System.out.println("test10_serviceSilent: bs created");
		assertContainsAll(bs, "output", Sets.newHashSet(l("input1_processed")));

		serviceResolver.shutDown();
	}

	@Test
	@Disabled("test is flaky - see https://github.com/eclipse/rdf4j/issues/3160")
	public void test11_errorHandling() throws Exception {

		assumeSparqlEndpoint();

		/*
		 * test select query where SERVICE is not part of federation and produces error
		 */
		prepareTest(Arrays.asList("/tests/data/data1.ttl", "/tests/data/data2.ttl", "/tests/data/data3.ttl",
				"/tests/data/data4.ttl"));
		Endpoint endpoint1 = federationContext().getEndpointManager().getEndpointByName("http://endpoint1");
		fedxRule.removeEndpoint(endpoint1);

		// run a simple SERVICE query
		repoSettings(1).resetOperationsCounter();
		repoSettings(1).setFailAfter(0);
		String query_a = readQueryString("/tests/service/query11_error_a.rq");

		Assertions.assertThrows(QueryEvaluationException.class, () -> {
			Repositories.tupleQueryNoTransaction(fedxRule.repository, query_a,
					iter -> QueryResults.asList(iter));
		});

		// run query where service does not produce errors
		String query_b = readQueryString("/tests/service/query11_error_b.rq");
		repoSettings(1).setFailAfter(-1);
		List<BindingSet> bs = Repositories.tupleQueryNoTransaction(fedxRule.repository, query_b,
				iter -> QueryResults.asList(iter));
		Assertions
				.assertEquals(Sets.newHashSet("Person2", "Person5"),
						bs.stream()
								.map(b -> b.getValue("name").stringValue())
								.collect(Collectors.toSet()));

		// re-run, but now simulate errors
		repoSettings(1).resetOperationsCounter();
		repoSettings(1).setFailAfter(1);
		Assertions.assertThrows(QueryEvaluationException.class, () -> {
			Repositories.tupleQueryNoTransaction(fedxRule.repository, query_b,
					iter -> QueryResults.asList(iter));
		});

	}

	static class TestSparqlFederatedService extends SPARQLFederatedService {

		AtomicInteger serviceRequestCount = new AtomicInteger(0);
		AtomicInteger boundJoinRequestCount = new AtomicInteger(0);

		public TestSparqlFederatedService(String serviceUrl, HttpClientSessionManager client) {
			super(serviceUrl, client);
		}

		@Override
		public CloseableIteration<BindingSet> select(Service service,
				Set<String> projectionVars, BindingSet bindings, String baseUri) throws QueryEvaluationException {
			serviceRequestCount.incrementAndGet();
			return super.select(service, projectionVars, bindings, baseUri);
		}

		@Override
		public CloseableIteration<BindingSet> evaluate(Service service,
				CloseableIteration<BindingSet> bindings, String baseUri)
				throws QueryEvaluationException {
			boundJoinRequestCount.incrementAndGet();
			return super.evaluate(service, bindings, baseUri);
		}

	}
}
