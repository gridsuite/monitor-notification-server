/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.monitor.notification.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Level;

import static java.util.stream.Collectors.toList;

/**
 * A WebSocketHandler that sends messages from a broker to websockets opened by clients, interleaving with pings to keep connections open.
 * <p>
 * Spring Cloud Stream gets the consumeNotification bean and calls it with the
 * flux from the broker. We call publish and connect to subscribe immediately to the flux
 * and multicast the messages to all connected websockets and to discard the messages when
 * no websockets are connected.
 *
 * @author Jon Harper <jon.harper at rte-france.com>
 */
@Component
public class MonitorNotificationWebSocketHandler implements WebSocketHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MonitorNotificationWebSocketHandler.class);
    private static final String CATEGORY_BROKER_INPUT = MonitorNotificationWebSocketHandler.class.getName() + ".messages.input-broker";
    private static final String CATEGORY_WS_OUTPUT = MonitorNotificationWebSocketHandler.class.getName() + ".messages.output-websocket";
    static final String HEADER_USER_ID = "userId";
    static final String HEADER_UPDATE_TYPE = "updateType";
    static final String HEADER_TIMESTAMP = "timestamp";
    static final String HEADER_ERROR = "error";

    static final String USERS_METER_NAME = "app.users";
    static final String USER_TAG = "user";

    private final ObjectMapper jacksonObjectMapper;

    private final int heartbeatInterval;

    private final Map<String, Integer> userConnections = new ConcurrentHashMap<>();

    public MonitorNotificationWebSocketHandler(ObjectMapper jacksonObjectMapper, @Value("${notification.websocket.heartbeat.interval:30}") int heartbeatInterval) {
        this.jacksonObjectMapper = jacksonObjectMapper;
        this.heartbeatInterval = heartbeatInterval;
    }

    Flux<Message<String>> flux;

    @Bean
    public Consumer<Flux<Message<String>>> consumeNotification() {
        return f -> {
            ConnectableFlux<Message<String>> c = f.log(CATEGORY_BROKER_INPUT, Level.FINE).publish();
            this.flux = c;
            c.connect();
            // Force connect 1 fake subscriber to consumme messages as they come.
            // Otherwise, reactorcore buffers some messages (not until the connectable flux had
            // at least one subscriber). Is there a better way ?
            c.subscribe();
        };
    }

    /**
     * map from the broker flux to the filtered flux for one websocket client, extracting only relevant fields.
     */
    private Flux<WebSocketMessage> notificationFlux(WebSocketSession webSocketSession) {
        return flux.map(m -> {
            try {
                return jacksonObjectMapper.writeValueAsString(Map.of(
                        "payload", m.getPayload(),
                        "headers", toResultHeader(m.getHeaders())));
            } catch (JsonProcessingException e) {
                throw new MonitorNotificationServerRuntimeException(e.toString());
            }
        }).log(CATEGORY_WS_OUTPUT, Level.FINE).map(webSocketSession::textMessage);
    }

    private static Map<String, Object> toResultHeader(Map<String, Object> messageHeader) {
        var resHeader = new HashMap<String, Object>();
        resHeader.put(HEADER_TIMESTAMP, messageHeader.get(HEADER_TIMESTAMP));
        resHeader.put(HEADER_UPDATE_TYPE, messageHeader.get(HEADER_UPDATE_TYPE));
        resHeader.put("processType", messageHeader.get("processType"));
        resHeader.put("processExecutionId", messageHeader.get("processExecutionId"));

        passHeader(messageHeader, resHeader, HEADER_ERROR);
        passHeader(messageHeader, resHeader, HEADER_USER_ID); // to filter the display of error messages in the front end

        return resHeader;
    }

    private static void passHeader(Map<String, Object> messageHeader, HashMap<String, Object> resHeader, String headerName) {
        if (messageHeader.get(headerName) != null) {
            resHeader.put(headerName, messageHeader.get(headerName));
        }
    }

    /**
     * A heartbeat flux sending websockets pings
     */
    private Flux<WebSocketMessage> heartbeatFlux(WebSocketSession webSocketSession) {
        return Flux.interval(Duration.ofSeconds(heartbeatInterval)).map(n -> webSocketSession
                .pingMessage(dbf -> dbf.wrap((webSocketSession.getId() + "-" + n).getBytes(StandardCharsets.UTF_8))));
    }

    public Flux<WebSocketMessage> receive(WebSocketSession webSocketSession) {
        return webSocketSession.receive()
                .doOnNext(webSocketMessage -> {
                    //if it's not the heartbeat
                    if (webSocketMessage.getType().equals(WebSocketMessage.Type.TEXT)) {
                        String wsPayload = webSocketMessage.getPayloadAsText();
                        LOGGER.debug("Message received : {} by session {}", wsPayload, webSocketSession.getId());
                    }
                });
    }

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        return webSocketSession
                .send(notificationFlux(webSocketSession).mergeWith(heartbeatFlux(webSocketSession)))
                .and(receive(webSocketSession))
                .doFirst(() -> updateConnectionMetrics(webSocketSession))
                .doFinally(s -> updateDisconnectionMetrics(webSocketSession));
    }

    private void updateConnectionMetrics(WebSocketSession webSocketSession) {
        var userId = webSocketSession.getHandshakeInfo().getHeaders().getFirst(HEADER_USER_ID);
        LOGGER.info("New websocket connection id={} for user={}", webSocketSession.getId(), userId);
        userConnections.compute(userId, (k, v) -> (v == null) ? 1 : v + 1);
    }

    private void updateDisconnectionMetrics(WebSocketSession webSocketSession) {
        var userId = webSocketSession.getHandshakeInfo().getHeaders().getFirst(HEADER_USER_ID);
        LOGGER.info("Websocket disconnection id={} for user={}", webSocketSession.getId(), userId);
        userConnections.computeIfPresent(userId, (k, v) -> v > 1 ? v - 1 : null);
    }
}
