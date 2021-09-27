/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.adhoc.flight.sql;

import java.io.IOException;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.example.FlightSqlClientDemoApp;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Dremio's Flight SQL Client Demo CLI Application.
 */
public class DremioFlightSqlClientDemoApp extends FlightSqlClientDemoApp {

  public static void main(final String[] args) {
    DremioFlightSqlClientDemoApp thisApp = new DremioFlightSqlClientDemoApp();

    thisApp.addRequiredOption("host", "Host to connect to");
    thisApp.addRequiredOption("port", "Port to connect to");
    thisApp.addRequiredOption("command", "Method to run");
    thisApp.addRequiredOption("username", "Auth username");
    thisApp.addRequiredOption("password", "Auth password");

    thisApp.addOptionalOption("query", "Query");
    thisApp.addOptionalOption("catalog", "Catalog");
    thisApp.addOptionalOption("schema", "Schema");
    thisApp.addOptionalOption("table", "Table");

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(thisApp.options, args);
      thisApp.executeApp(cmd);

    } catch (final ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("DremioFlightSqlClientDemoApp -host localhost -port 32010 ...", thisApp.options);
      System.exit(1);
    } catch (final IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public CallOption[] getCallOptions() {
    return super.getCallOptions();
  }

  /**
   * Adds a {@link CallOption} to the current array.
   */
  public void addCallOption(final CallOption optionToAdd) {
    callOptions.add(optionToAdd);
  }

  /**
   * Gets the current {@link BufferAllocator}.
   *
   * @return current {@link BufferAllocator}.
   */
  public BufferAllocator getAllocator() {
    return allocator;
  }

  /**
   * Calls {@link DremioFlightSqlClientDemoApp#createFlightSqlClient(String, String, String, String)}
   * in order to create a {@link FlightSqlClient} to be used in future calls,
   * and then calls {@link DremioFlightSqlClientDemoApp#executeCommand(CommandLine)}
   * to execute the command parsed at execution.
   *
   * @param cmd Parsed {@link CommandLine}; often the result of {@link DefaultParser#parse(Options, String[])}.
   */
  public void executeApp(CommandLine cmd) throws IOException {
    createFlightSqlClient(
        cmd.getOptionValue("host").trim(), cmd.getOptionValue("port").trim(),
        cmd.getOptionValue("username").trim(), cmd.getOptionValue("password").trim());
    executeCommand(cmd);
  }

  /**
   * Creates a {@link FlightSqlClient} to be used with the example methods.
   *
   * @param host client's hostname.
   * @param port client's port.
   * @param user client's username auth.
   * @param pass client's password auth.
   */
  public void createFlightSqlClient(final String host, final String port, final String user, final String pass) {
    final ClientIncomingAuthHeaderMiddleware.Factory factory =
        new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
    final FlightClient client = FlightClient.builder()
        .allocator(getAllocator())
        .location(Location.forGrpcInsecure(host, Integer.parseInt(port)))
        .intercept(factory)
        .build();
    client.handshake(new CredentialCallOption(new BasicAuthCredentialWriter(user, pass)));
    addCallOption(factory.getCredentialCallOption());
    flightSqlClient = new FlightSqlClient(client);
  }
}
