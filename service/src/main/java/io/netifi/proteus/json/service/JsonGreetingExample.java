package io.netifi.proteus.json.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netifi.proteus.json.om.Bye;
import io.netifi.proteus.json.om.Greeting;
import io.netifi.proteus.json.om.Hello;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class JsonGreetingExample extends AbstractRSocket {
  private final ObjectMapper mapper;

  public JsonGreetingExample() {
    this.mapper = new ObjectMapper();
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      String data = payload.getDataUtf8();
      String metadata = payload.getMetadataUtf8();
      Greeting greeting;
      switch (metadata) {
        case "hi":
          Hello hello = mapper.readValue(data, Hello.class);
          greeting = new Greeting();
          greeting.setGreeting("Hello " + hello.getName());
          break;
        case "bye":
          Bye bye = mapper.readValue(data, Bye.class);
          greeting = new Greeting();
          greeting.setGreeting("Bye " + bye.getName());
          break;
        default:
          return Mono.error(new IllegalArgumentException("unknown metadata -> " + metadata));
      }

      String json = toJson(greeting);
      return Mono.just(DefaultPayload.create(json));
    } catch (Throwable e) {
      return Mono.error(e);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      String data = payload.getDataUtf8();
      String metadata = payload.getMetadataUtf8();
      Greeting greeting;
      switch (metadata) {
        case "hi":
          Hello hello = mapper.readValue(data, Hello.class);
          greeting = new Greeting();
          greeting.setGreeting("Hello " + hello.getName());
          break;
        case "bye":
          Bye bye = mapper.readValue(data, Bye.class);
          greeting = new Greeting();
          greeting.setGreeting("Bye " + bye.getName());
          break;
        default:
          return Flux.error(new IllegalArgumentException("unknown metadata -> " + metadata));
      }

      return Flux.range(1, 1_000)
          .map(
              i -> {
                greeting.setTimes(i);
                return DefaultPayload.create(toJson(greeting));
              });
    } catch (Throwable e) {
      return Flux.error(e);
    }
  }

  private String toJson(Greeting greeting) {
    try {
      return mapper.writeValueAsString(greeting);
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }
}
