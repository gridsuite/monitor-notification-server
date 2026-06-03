/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.monitor.notification.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import static org.gridsuite.monitor.notification.server.MonitorNotificationWebSocketHandler.HEADER_ERROR;
import static org.gridsuite.monitor.notification.server.MonitorNotificationWebSocketHandler.HEADER_PROCESS_EXECUTION_ID;
import static org.gridsuite.monitor.notification.server.MonitorNotificationWebSocketHandler.HEADER_PROCESS_TYPE;
import static org.gridsuite.monitor.notification.server.MonitorNotificationWebSocketHandler.HEADER_UPDATE_TYPE;
import static org.gridsuite.monitor.notification.server.MonitorNotificationWebSocketHandler.HEADER_USER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MonitorNotificationWebSocketHandlerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DefaultDataBufferFactory dataBufferFactory = new DefaultDataBufferFactory();

    private WebSocketSession webSocketSession;

    @BeforeEach
    void setUp() {
        webSocketSession = mock(WebSocketSession.class);

        when(webSocketSession.getId()).thenReturn("test-session");
        when(webSocketSession.receive()).thenReturn(Flux.empty());
        when(webSocketSession.send(any())).thenReturn(Mono.empty());
        when(webSocketSession.textMessage(any())).thenAnswer(invocation ->
            new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(invocation.getArgument(0, String.class).getBytes(StandardCharsets.UTF_8)))
        );
        when(webSocketSession.pingMessage(any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Function<DataBufferFactory, DataBuffer> payloadFactory = invocation.getArgument(0, Function.class);
            return new WebSocketMessage(WebSocketMessage.Type.PING, payloadFactory.apply(dataBufferFactory));
        });
    }

    @Test
    void testShouldSerializeNotificationWithSupportedHeadersOnly() throws Exception {
        MonitorNotificationWebSocketHandler handler = new MonitorNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE);
        AtomicReference<FluxSink<Message<String>>> sinkReference = new AtomicReference<>();
        handler.consumeNotification().accept(Flux.create(sinkReference::set));

        handler.handle(webSocketSession);

        Flux<WebSocketMessage> outboundMessages = captureOutboundMessages();
        List<String> messages = new ArrayList<>();
        Disposable subscription = outboundMessages
            .map(WebSocketMessage::getPayloadAsText)
            .subscribe(messages::add);

        sinkReference.get().next(new GenericMessage<>("payload", Map.of(
            HEADER_UPDATE_TYPE, "PROCESS_UPDATED",
            HEADER_PROCESS_TYPE, "PROCESS_TYPE",
            HEADER_PROCESS_EXECUTION_ID, "exec-id",
            HEADER_ERROR, "error",
            HEADER_USER_ID, "userId",
            "ignored", "value"
        )));
        sinkReference.get().complete();

        assertEquals(1, messages.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = objectMapper.readValue(messages.get(0), Map.class);
        subscription.dispose();

        assertEquals("payload", payload.get("payload"));
        assertInstanceOf(Map.class, payload.get("headers"));

        @SuppressWarnings("unchecked")
        Map<String, Object> headers = (Map<String, Object>) payload.get("headers");
        assertEquals("PROCESS_UPDATED", headers.get(HEADER_UPDATE_TYPE));
        assertEquals("PROCESS_TYPE", headers.get(HEADER_PROCESS_TYPE));
        assertEquals("exec-id", headers.get(HEADER_PROCESS_EXECUTION_ID));
        assertEquals("error", headers.get(HEADER_ERROR));
        assertEquals("userId", headers.get(HEADER_USER_ID));
        assertFalse(headers.containsKey("ignored"));
    }

    @Test
    void testShouldEmitHeartbeatMessages() {
        MonitorNotificationWebSocketHandler handler = new MonitorNotificationWebSocketHandler(objectMapper, 1);
        handler.consumeNotification().accept(Flux.empty());

        handler.handle(webSocketSession);

        Flux<WebSocketMessage> outboundMessages = captureOutboundMessages();
        WebSocketMessage heartbeat = outboundMessages.blockFirst(Duration.ofSeconds(2));
        assertEquals(WebSocketMessage.Type.PING, heartbeat.getType());
        assertEquals("test-session-0", heartbeat.getPayloadAsText());
    }

    @Test
    void testReceiveShouldReturnIncomingMessages() {
        WebSocketMessage textMessage = new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap("hello".getBytes(StandardCharsets.UTF_8)));
        WebSocketMessage binaryMessage = new WebSocketMessage(WebSocketMessage.Type.BINARY, dataBufferFactory.wrap(new byte[] {1, 2, 3}));
        when(webSocketSession.receive()).thenReturn(Flux.just(textMessage, binaryMessage));

        List<WebSocketMessage> receivedMessages = new MonitorNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE)
            .receive(webSocketSession)
            .collectList()
            .block(Duration.ofSeconds(1));

        assertEquals(2, receivedMessages.size());
        assertSame(textMessage, receivedMessages.get(0));
        assertSame(binaryMessage, receivedMessages.get(1));
    }

    @Test
    void testConsumeNotificationShouldNotReplayMessagesSentIfNoClientSubscribed() throws Exception {
        MonitorNotificationWebSocketHandler handler = new MonitorNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE);
        AtomicReference<FluxSink<Message<String>>> sinkReference = new AtomicReference<>();
        handler.consumeNotification().accept(Flux.create(sinkReference::set));

        FluxSink<Message<String>> sink = sinkReference.get();
        sink.next(new GenericMessage<>("discarded", Map.of(HEADER_UPDATE_TYPE, "FIRST")));  // should be discarded, no client connected

        handler.handle(webSocketSession);

        Flux<WebSocketMessage> outboundMessages = captureOutboundMessages();
        List<String> messages = new ArrayList<>();
        Disposable subscription = outboundMessages
            .map(WebSocketMessage::getPayloadAsText)
            .subscribe(messages::add);

        sink.next(new GenericMessage<>("delivered", Map.of(HEADER_UPDATE_TYPE, "SECOND")));
        sink.complete();

        @SuppressWarnings("unchecked")
        Map<String, Object> payload = objectMapper.readValue(messages.get(0), Map.class);
        subscription.dispose();
        assertEquals("delivered", payload.get("payload"));

        @SuppressWarnings("unchecked")
        Map<String, Object> headers = (Map<String, Object>) payload.get("headers");
        assertEquals("SECOND", headers.get(HEADER_UPDATE_TYPE));
    }

    @SuppressWarnings("unchecked")
    private Flux<WebSocketMessage> captureOutboundMessages() {
        ArgumentCaptor<Flux<WebSocketMessage>> argumentCaptor = ArgumentCaptor.forClass(Flux.class);
        verify(webSocketSession).send(argumentCaptor.capture());
        return argumentCaptor.getValue();
    }
}
