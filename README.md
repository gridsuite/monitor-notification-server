# Monitor Notification Server

[![Actions Status](https://github.com/gridsuite/monitor-notification-server/workflows/CI/badge.svg)](https://github.com/gridsuite/monitor-notification-server/actions)
[![Coverage Status](https://sonarcloud.io/api/project_badges/measure?project=org.gridsuite%3Amonitor-notification-server&metric=coverage)](https://sonarcloud.io/component_measures?id=org.gridsuite%3Amonitor-notification-server&metric=coverage)
[![MPL-2.0 License](https://img.shields.io/badge/license-MPL_2.0-blue.svg)](https://www.mozilla.org/en-US/MPL/2.0/)

## Description
Monitor Notification Server is the GridMonitor notification service. It consumes process execution updates from RabbitMQ and exposes them to GridMonitor clients through a WebSocket endpoint.

## Functional Scope

- Consume GridMonitor update messages from the `monitor.update` RabbitMQ destination.
- Broadcast updates to all connected WebSocket clients.
- Forward the update payload and selected message headers needed by the frontend.
- Send periodic WebSocket ping frames to keep client connections alive.

## WebSocket API

The service exposes one WebSocket endpoint:

```text
/notify
```

Each outbound text message is a JSON object with the consumed message payload and a filtered header set:

```json
{
  "payload": "...",
  "headers": {
    "timestamp": "...",
    "updateType": "...",
    "processType": "...",
    "processExecutionId": "...",
    "error": "...",
    "userId": "..."
  }
}
```

`processType`, `error`, and `userId` are included only when present in the consumed RabbitMQ message headers. The `userId` header is forwarded so the frontend can filter user-scoped error messages.
