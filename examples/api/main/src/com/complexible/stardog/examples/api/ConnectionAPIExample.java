/*
 * Copyright (c) 2010-2018 Stardog Union. <https://stardog.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.complexible.stardog.examples.api;

import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.util.Static;

import com.complexible.common.rdf.query.resultio.TextTableQueryResultWriter;
import com.complexible.stardog.Stardog;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.api.Getter;
import com.complexible.stardog.api.GraphQuery;
import com.complexible.stardog.api.SelectQuery;
import com.complexible.stardog.api.UpdateQuery;
import com.complexible.stardog.api.admin.AdminConnection;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import com.complexible.stardog.api.admin.DatabaseBuilder;
import com.stardog.stark.IRI;
import com.stardog.stark.Statement;
import com.stardog.stark.Values;
import com.stardog.stark.io.RDFFormats;
import com.stardog.stark.io.RDFWriters;
import com.stardog.stark.query.SelectQueryResult;
import com.stardog.stark.query.io.QueryResultFormats;
import com.stardog.stark.query.io.QueryResultWriters;
import com.stardog.stark.vocabs.RDF;

import java.time.format.DateTimeFormatter;  
import java.time.LocalDateTime;    
import com.complexible.common.base.Option;

/**
 * <p>Example code illustrating use of the Stardog Connection API</p>
 *
 * @author Michael Grove
 * @version 6.0
 * @since 0.4
 */
public class ConnectionAPIExample {

	public static void printTime(String msg) {    
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");  
		LocalDateTime now = LocalDateTime.now();  
		System.out.println("INFO " + dtf.format(now) + " - " + msg);  
	   }

	// Using the SNARL API
	// -------------------
	// In this example we'll walk through the basic usage of the Stardog Native API for the RDF Language (SNARL)
	// API, which is the preferred way to interact with Stardog.  This will show how to use both the administrative
	// and client APIs to perform some basic operations.
	public static void main(String[] args) throws Exception {

		try {
			// Using AdminConnection
			// ---------------------
			// Now that the server is running, we want to create a connection to the DBMS itself so we can perform
			// some administrative actions, namely, creating a new database to use for the purpose of this example.
			// We need to create a connection to perform administrative actions, so we can use the `AdminConnectionConfiguration`
			// utility class for opening the connection.
			//
			// Most operations supported by the DBMS require specific permissions, so either an admin account
			// is required, or a user who has been granted the ability to perform the actions.  You can learn
			// more about this in the [Security chapter](http://docs.stardog.com/security).


			// TODO check that the server is alive and available here if it is not throw an error

			try (AdminConnection aAdminConnection = AdminConnectionConfiguration.toServer("http://localhost:5820")
			                                                                    .credentials("admin", "admin")
			                                                                    .connect()) {
				// With our admin connection, we're able to see if the database for this example already exists, and
				// if it does, we want to drop it and re-create so that we can run the example from a clean database.
				if (aAdminConnection.list().contains("testConnectionAPI")) {
					aAdminConnection.drop("testConnectionAPI");
				}

				aAdminConnection.newDatabase("testConnectionAPI").create();

				// Using the SNARL API
				// -------------------
				// Now that we've created our database for the example, let's open a connection to it.  For that we use the
				// [ConnectionConfiguration](http://docs.stardog.com/javadoc/snarl/com/complexible/stardog/api/ConnectionConfiguration.html)
				// to configure and open a new connection to a database.
				//
				// We'll use the configuration to specify which database we want to connect to as well as our login information,
				// then we can obtain a new connection.

				try (Connection aConn = ConnectionConfiguration
					                        .to("testConnectionAPI")
					                        .credentials("admin", "admin")
											.server("http://localhost:5820")
					                        .connect()) {
					// All changes to a database *must* be performed within a transaction.  We want to add some data to the database
					// so we can begin firing off some queries, so first, we'll start a new transaction.
					aConn.begin();

					// The SNARL API provides fluent objects for adding & removing data from a database.  Here we'll use the
					// [Adder](http://docs.stardog.com/javadoc/snarl/com/complexible/stardog/api/Adder.html) to read in an N3 file
					// from disk containing a small subset of the SP2B dataset.  Actually, for RDF data coming from a stream or from
					// disk, we'll use the helper class [IO](http://docs.stardog.com/javadoc/snarl/com/complexible/stardog/api/IO.html)
					// for this task.  `IO` will automatically close the stream once the data has been read.
					aConn.add().io()
					     .format(RDFFormats.TURTLE)
					     .stream(new FileInputStream("data/sp2b.ttl"));

					// You're not restricted to adding, or removing, data from a file.  You can create `Statement` objects
					// containing information you want to add or remove from the database and make the modification wrt
					// that graph.  Here we'll add a statement that we want added to our database.
					Statement aStatement = Values.statement(Values.iri("uri:subj"),
							Values.iri("urn:pred"),
							Values.iri("urn:obj"),
							Values.iri("urn:test:context"));

					// We can easily add this Statement to our database. We also could have used the `graph` method
					// to add a collection of them all at once.
					aConn.add().statement(aStatement);

					// Now that we're done adding data to the database, we can go ahead and commit the transaction.
					aConn.commit();



					// // Using the ADD graph management SPARQL operator

					System.out.println("Using the ADD graph management SPARQL operator");
					printTime("starting connection") ;

					aConn.begin();


					
					// aConn.update("ADD <virtual://vg-trade> TO DEFAULT").timeout(10000).execute();
					GraphQuery updateQuery = aConn.graph("ADD DEFAULT TO <https://stardog/examples/data_add_test>");

					printTime("starting update");
					

					// This is the line that throw the "Cannot execute update query on read endpoint" Exception
					// TODO - fiqure out how to have read endpoint
					// updateQuery.execute();

					printTime("update complete");

					aConn.commit();



					aConn.begin();

					// aConn.update("ADD <tag:stardog:api:context:default> TO <https://stardog/examples/data_add_test>").execute();
					
					SelectQuery addQuery = aConn.select("SELECT (count(*) AS ?count) { GRAPH <https://stardog/examples/data_add_test> { ?s ?p ?o }}");

					try(SelectQueryResult aResult = addQuery.execute()) {
						System.out.println("Check that the ADD command worked");

						QueryResultWriters.write(aResult, System.out, TextTableQueryResultWriter.FORMAT);
					}

					aConn.commit();
					


					aConn.begin();

					// checks connection to virtual graph from the Java API
					SelectQuery tradeVirtualGraphTest = aConn.select("SELECT (count(*) AS ?count) { GRAPH <virtual://vg-trade> { ?subject rdf:type :trade }}");

					tradeVirtualGraphTest.limit(10);

					try(SelectQueryResult aResult = tradeVirtualGraphTest.execute()) {
						System.out.println("Number of Trades in the Virtual Graph");

						QueryResultWriters.write(aResult, System.out, TextTableQueryResultWriter.FORMAT);
					}

					printTime("starting commit");
					aConn.commit();

					printTime("commit complete"); 



					// Removing data from a database is just as easy.  Again, we need to start a transaction before making any changes.
					aConn.begin();

					// Now we'll use the [Remover](http://docs.stardog.com/javadoc/snarl/com/complexible/stardog/api/Remover.html) to
					// remove some data from the database.  `Remover` has a very similar API to `Adder`, so this code should look
					// somewhat familiar.  It has many of the same methods as `Adder`, the only difference is that they'll cause
					// the triples to be removed instead of added.
					aConn.remove().io()
					     .format(RDFFormats.N3)
					     .file(Paths.get("data/remove_data.nt"));

					// Lastly, we'll commit the changes.
					aConn.commit();

					// A SNARL connection provides [parameterized queries](http://docs.stardog.com/javadoc/snarl/com/complexible/stardog/api/Query.html)
					// which you can use to easily build and execute SPARQL queries against the database.  First, let's create a simple
					// query that will get all of the statements in the database.
					SelectQuery aQuery = aConn.select("select * where { ?s ?p ?o }");

					// But getting *all* the statements is kind of silly, so let's actually specify a limit, we only want 10 results.
					aQuery.limit(10);

					// We can go ahead and execute this query which will give us a result set.  Once we have our result set, we can do
					// something interesting with the results.
					// NOTE: We use try-with-resources here to ensure that our results sets are always closed.
					try(SelectQueryResult aResult = aQuery.execute()) {
						System.out.println("The first ten results...");

						QueryResultWriters.write(aResult, System.out, TextTableQueryResultWriter.FORMAT);
					}

					// `Query` objects are easily parameterized; so we can bind the "s" variable in the previous query with a specific value.
					// Queries should be managed via the parameterized methods, rather than created by concatenating strings together,
					// because that is not only more readable, it helps avoid SPARQL injection attacks.
					IRI aURI = Values.iri("http://localhost/publications/articles/Journal1/1940/Article1");
					aQuery.parameter("s", aURI);

					// Now that we've bound 's' to a specific value, we're not going to pull down the entire database with our query
					// so we can go head and remove the limit and get all the results.
					aQuery.limit(SelectQuery.NO_LIMIT);

					// We've made our modifications, so we can re-run the query to get a new result set and see the difference in the results.
					try(SelectQueryResult aResult = aQuery.execute()) {
						System.out.println("\nNow a particular slice...");

						QueryResultWriters.write(aResult, System.out, TextTableQueryResultWriter.FORMAT);
					}

					// The previous query was just getting the statements in which the value of `aURI` is the subject.  We can get the
					// same results just as easily via the [Getter](http://docs.stardog.com/javadoc/snarl/com/complexible/stardog/api/Getter.html)
					// interface.  `Getter` is designed to make it easy to list statements matching specific criteria; it's analogous to
					// `listStatements` or `match` in the Jena & Sesame APIs respectively.
					//
					// So here we'll create a `Getter` to obtain the list of statements with `aURI` as the subject.  If we print those
					// out we'll see that we've retrieved the same results as the query we just ran.
					System.out.println("\nOr you can use a getter to do the same thing...");

					aConn.get().subject(aURI)
					     .statements()
					     .forEach(System.out::println);

					// `Getter` objects are parameterizable just like `Query`, so you can easily modify and re-use them to change
					// what slice of the database you'll retrieve.
					Getter aGetter = aConn.get();

					// We created a new `Getter`, if we iterated over its results now, we'd iterate over the whole database; not ideal.  So
					// we will bind the predicate to `rdf:type` and now if we call any of the iteration methods on the `Getter` we'd only
					// pull back statements whose predicate is `rdf:type`
					aGetter.predicate(RDF.TYPE);

					// We can also bind the subject and get a specific type statement, in this case, we'll get all the type triples
					// for *this* individual.  In our example, that'll be a single triple.
					aGetter.subject(aURI);

					System.out.println("\nJust a single statement now...");

					aGetter.statements()
					       .forEach(System.out::println);

					// `Getter` objects are stateful, so we can remove the filter on the predicate position by setting it back to null.
					aGetter.predicate(null);

					// Subject is still bound to the value of `aURI` so we can use the `statements` method of `Getter`
					// to stream out the triples where `aURI` is the subject, effectively performing a basic describe query.
					Stream<Statement> aStatements = aGetter.statements();

					System.out.println("\nFinally, the same results as earlier, but as a graph...");

					RDFWriters.write(System.out, RDFFormats.TURTLE, aStatements.collect(Collectors.toList()));

				}
				finally {
					// remove the database
					if (aAdminConnection.list().contains("testConnectionAPI")) {
						aAdminConnection.drop("testConnectionAPI");

						System.out.println("\n\nConnectionAPIExample ran successfully");
					}
				}
			}
		}
		finally {
			
		}
	}
}
