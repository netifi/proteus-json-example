package io.netifi.proteus.json.service;

import io.netifi.proteus.Proteus;

/** Starts the Proteus Quickstart Server */
public class Main {

  public static void main(String... args) throws Exception {
    // Build Netifi Connection
    Proteus netifi =
        Proteus.builder()
            .group("json.example.service") // Group name of service
            .accessKey(9007199254740991L)
            .accessToken("kTBDVtfRBO4tHOnZzSyY5ym2kfY=")
            .host("localhost") // Proteus Broker Host
            .port(8001) // Proteus Broker Port
            .build();

    netifi.addNamedRSocket("JsonGreetingExample", new JsonGreetingExample());

    netifi.addNamedRSocket("JsonAddingExample", new JsonAddingExample());

    // Keep the Service Running
    Thread.currentThread().join();
  }
}
