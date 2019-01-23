package io.netifi.proteus.json.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netifi.proteus.json.om.Delta;
import io.netifi.proteus.json.om.Result;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.internal.SwitchTransformFlux;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonAddingExample extends AbstractRSocket {
  private final AtomicInteger total;
  private final ObjectMapper mapper;

  public JsonAddingExample() {
    this.total = new AtomicInteger();
    this.mapper = new ObjectMapper();
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      String data = payload.getDataUtf8();
      String metadata = payload.getMetadataUtf8();
      switch (metadata) {
        case "add":
          {
            Delta delta = mapper.readValue(data, Delta.class);
            int i = total.addAndGet(delta.getDelta());
            Result result = new Result();
            result.setResult(i);
            String json = toJson(result);
            return Mono.just(ByteBufPayload.create(json));
          }
        case "multiple":
          {
            Delta delta = mapper.readValue(data, Delta.class);
            int i = total.updateAndGet(operand -> operand * delta.getDelta());
            Result result = new Result();
            result.setResult(i);
            String json = toJson(result);
            return Mono.just(ByteBufPayload.create(json));
          }
        default:
          return Mono.error(new IllegalArgumentException("unknown metadata -> " + metadata));
      }
    } catch (Throwable e) {
      return Mono.error(e);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    // Switch the stream based on the first payload
    SwitchTransformFlux<Payload, Payload> transform =
        new SwitchTransformFlux<Payload, Payload>(
            payloads,
            (p, p1) -> {
              String metadata = p.getMetadataUtf8();
              switch (metadata) {
                case "add":
                  return p1.map(
                      payload -> {
                        Delta delta = null;
                        try {
                          delta = mapper.readValue(payload.getDataUtf8(), Delta.class);
                        } catch (IOException e) {
                          throw Exceptions.propagate(e);
                        }
                        int i = total.addAndGet(delta.getDelta());
                        Result result = new Result();
                        result.setResult(i);
                        String json = toJson(result);
                        return ByteBufPayload.create(json);
                      });
                case "multiple":
                  return p1.map(
                      payload -> {
                        int i =
                            total.updateAndGet(
                                operand -> {
                                  Delta delta = null;
                                  try {
                                    delta = mapper.readValue(payload.getDataUtf8(), Delta.class);
                                  } catch (IOException e) {
                                    throw Exceptions.propagate(e);
                                  }

                                  return delta.getDelta() * operand;
                                });
                        Result result = new Result();
                        result.setResult(i);
                        String json = toJson(result);
                        return ByteBufPayload.create(json);
                      });
                default:
                  return Mono.error(
                      new IllegalArgumentException("unknown metadata -> " + metadata));
              }
            });
    
    return transform;
  }

  private String toJson(Result result) {
    try {
      return mapper.writeValueAsString(result);
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }
}
