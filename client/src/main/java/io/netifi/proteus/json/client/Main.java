package io.netifi.proteus.json.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netifi.proteus.Proteus;
import io.netifi.proteus.json.om.Delta;
import io.netifi.proteus.json.om.Hello;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.DefaultPayload;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;

/** Starts the Proteus Quickstart Client */
public class Main {
  private static final Logger logger = LogManager.getLogger(Main.class);

  private static ObjectMapper mapper = new ObjectMapper();

  public static void main(String... args) throws Exception {

    // Build Netifi Proteus Connection
    Proteus netifi =
        Proteus.builder()
            .group("json.example.client") // Group name of client
            .accessKey(9007199254740991L)
            .accessToken("kTBDVtfRBO4tHOnZzSyY5ym2kfY=")
            .host("localhost") // Proteus Broker Host
            .port(8001) // Proteus Broker Port
            .build();

    // Connect to Netifi Proteus Platform
    RSocket jsonGreetingExample =
        netifi.groupNamedRSocket("JsonGreetingExample", "json.example.service");

    RSocket jsonAddingExample =
        netifi.groupNamedRSocket("JsonAddingExample", "json.example.service");

    Payload alice =
        jsonGreetingExample.requestResponse(ByteBufPayload.create(hello("Alice"), "hi")).block();

    logger.info(alice.getDataUtf8());

    alice =
        jsonGreetingExample.requestResponse(DefaultPayload.create(hello("Alice"), "bye")).block();

    logger.info(alice.getDataUtf8());

    Flux.merge(
            jsonGreetingExample.requestStream(DefaultPayload.create(hello("Bob"), "hi")),
            jsonGreetingExample.requestStream(DefaultPayload.create(hello("Bob"), "bye")))
        .limitRequest(20)
        .toIterable()
        .forEach(
            payload -> {
              logger.info(payload.getDataUtf8());
            });

    Payload result =
        jsonAddingExample.requestResponse(DefaultPayload.create(delta(1), "add")).block();

    logger.info(result.getDataUtf8());

    result = jsonAddingExample.requestResponse(DefaultPayload.create(delta(2), "multiple")).block();

    logger.info(result.getDataUtf8());

    jsonAddingExample
        .requestChannel(Flux.range(1, 10).map(i -> DefaultPayload.create(delta(i), "add")))
        .take(5)
        .toIterable()
        .forEach(payload -> logger.info(payload.getDataUtf8()));

    jsonAddingExample
        .requestChannel(Flux.range(1, 10).map(i -> DefaultPayload.create(delta(i), "multiple")))
        .take(5)
        .toIterable()
        .forEach(payload -> logger.info(payload.getDataUtf8()));
  }

  private static String hello(String name) throws Exception {
    Hello hello = new Hello();
    hello.setName(name);

    return mapper.writeValueAsString(hello);
  }

  private static String delta(int i) {
    try {
      Delta delta = new Delta();
      delta.setDelta(i);
      return mapper.writeValueAsString(delta);
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }
}
