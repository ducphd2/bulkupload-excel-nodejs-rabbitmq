const { Connection } = require("rabbitmq-client");

const RABBIT_SERVER_URL = process.env.RABBIT_SERVER_URL;

const rabbitMq = new Connection({
  url: RABBIT_SERVER_URL,
  // wait 1 to 30 seconds between connection retries
  retryLow: 1000,
  retryHigh: 30000,
});

rabbitMq.on("error", (err) => {
  console.error("Refused when connect to rabbitMQ", { context: { err } });
});

rabbitMq.on("connection", () => {
  console.info("Connection to rabbitMQ is successfully (re)established");
});

const sendMessage = async (message, queueName) => {
  const channel = await rabbitMq.acquire();
  channel.on("close", () => {
    console.error("RabbitMQ channel was closed");
  });
  await channel.basicPublish({ routingKey: queueName }, message);
  console.info(`Success to send msg to RabbitMQ queue ${queueName}`, {
    context: message,
  });
};

module.exports = {
  rabbitMq,
  sendMessage,
};
