# staking-queue-client

This document focuses on the usage of RabbitMQ for message processing in the 
Bitcoin staking system.

## Overview

Our RabbitMQ client is designed to handle robust message processing with 
mechanisms for error handling and message retry capabilities. The setup 
involves the following components:

- **Main Queues**: Primary queues where messages are initially sent and 
  processed.
- **Delayed Queues**: Queues used to temporarily hold messages that need to 
  be retried after a delay.
- **Dead Letter Exchange (DLX)**: A special exchange used to reroute 
  messages from main queues to delayed queues when processing fails or when 
  a message needs to be delayed.

## Configuration Details

### Dead Letter Exchange (DLX)

- **Purpose**: The DLX handles messages that fail during processing or messages 
that need to be retried after a specific delay.
- **Name**: `common_dlx`

### Main Queues

Main queues are created by initiating the `NewQueueClient`.
For example, in the Babylon mainnet phase 1, we have the following queues:

```go
const (
    ActiveStakingQueueName    string = "active_staking_queue"
    UnbondingStakingQueueName string = "unbonding_staking_queue"
    WithdrawStakingQueueName  string = "withdraw_staking_queue"
    ExpiredStakingQueueName   string = "expired_staking_queue"
    StakingStatsQueueName     string = "staking_stats_queue"
    BtcInfoQueueName          string = "btc_info_queue"
)
```

- **Configuration**:
  - Each main queue is configured with the DLX (`common_dlx`) as the 
    dead-letter exchange.
  - Messages that fail to process are sent to the DLX with a specific 
    routing key that directs them to the corresponding delayed queue. The 
    routing key is in the form of `{{queueName}}` + `_routing_key`

### Delayed Queues

For each queue that is created, we also auto-provision a corresponding 
delayed queue.  

The name of the delayed queue is in the form of `{{queueName}}` + `_delay`

- **Purpose**: Delayed queues hold messages for a predetermined time before 
  they are sent back to the main queue for reprocessing.
- **TTL (Time To Live)**: Each delayed queue is configured with a TTL. After 
  the TTL expires, messages are automatically sent back to the main queue.
- **Routing**: Messages are routed back to the main queue using the default 
  exchange, which routes messages based on the queue name.

## Re-queueing Messages

Re-queueing of messages occurs when the application is temporarily unable to 
process them for any reason. To manage this effectively, we have implemented 
a custom solution that delays the re-queueing of messages. This delay 
ensures that the service can process these messages correctly when it 
returns to a suitable state.

### Delayed Re-queueing Strategy

RabbitMQ does not natively support delayed message re-queueing, 
so we have devised a custom approach to handle this requirement:

- **Delayed Queues with TTL**: When the `ReQueueMessage` function is called, 
  the message is sent directly to a specially configured delay queue. This 
  delay queue has a predefined Time To Live (TTL), after which the message 
  is automatically redirected back to the main queue for processing.

- **Message Retry Tracking**: Each time a message is re-queued, we increment 
  its retry attempt count. This is accomplished by cloning the original 
  message into a new message with an incremented retry count. The original 
  message is then deleted from the queue after the new message is 
  successfully enqueued.

This system ensures that messages are not lost and are processed in an orderly 
manner once the application is ready to handle them again. It provides a robust 
solution to manage message processing failures and maintain service reliability 
even under varying system conditions.
