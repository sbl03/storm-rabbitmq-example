storm-rabbitmq-example
======================

Basic implementation of [ppat/storm-rabbitmq](https://github.com/ppat/storm-rabbitmq)

###Usage

1. Get host information for an existing RabbitMQ cluster, or [start a RabbitMQ server locally.](https://www.rabbitmq.com/download.html)
2. Then, it's as simple as importing the project and running the Main class. It will make a `storm-test-exchange` exchange, and a `storm-test-queue` queue, where it will produce and consume messages. All of the configuration can be changed at the top of the Main class.
