const { createRabbitMqConsumer, rabbitMq } = require("./rabbitMqHelper");
const queue = process.env.UPLOAD_FILE_QUEUE || "file-to-room-queue";
const ROOM_DEAD_LETTER = process.env.ROOM_DEAD_LETTER || "room-dlx";
const FILE_TO_ROOM_DEAD_LETTERS_EXCHANGES = (process.env.FILE_TO_ROOM_DEAD_LETTERS_EXCHANGES =
  process.env.FILE_TO_ROOM_DEAD_LETTERS_EXCHANGES || "file-to-room-dlx");

const RABBIT_NUMBER_MESSAGE_HANDLE_AT_SAME_TIME = process.env.RABBIT_NUMBER_MESSAGE_HANDLE_AT_SAME_TIME || 5;

let sqsRabbitMqAppQueueConsumer;

const messageHandler = async (message, options) => {
  console.log(message.body.message);
};

const createRabbitMqDeadLetterQueue = async () => {
  const channel = await rabbitMq.acquire();
  await channel.queueDeclare({ queue: ROOM_DEAD_LETTER });
};

const startRabbitMqConsumer = (options) => {
  if (!sqsRabbitMqAppQueueConsumer) {
    sqsRabbitMqAppQueueConsumer = rabbitMq.createConsumer(
      {
        queue,
        queueOptions: {
          durable: true,
          noAck: false,
          arguments: {
            "x-dead-letter-exchange": FILE_TO_ROOM_DEAD_LETTERS_EXCHANGES,
            "x-dead-letter-routing-key": FILE_TO_ROOM_DEAD_LETTERS_EXCHANGES,
            "x-overflow": "reject-publish",
            "x-queue-type": "quorum",
          },
        },
        qos: { prefetchCount: RABBIT_NUMBER_MESSAGE_HANDLE_AT_SAME_TIME },
        exchanges: [{ exchange: FILE_TO_ROOM_DEAD_LETTERS_EXCHANGES, type: "fanout" }],
        queueBindings: [
          {
            exchange: FILE_TO_ROOM_DEAD_LETTERS_EXCHANGES,
            queue: ROOM_DEAD_LETTER,
          },
        ],
      },
      async (msg) => messageHandler(msg, options)
    );

    sqsRabbitMqAppQueueConsumer.on("error", (error) => {
      console.error("Has error", { context: error });
      throw new Error();
    });
    createRabbitMqDeadLetterQueue();
  }
  return sqsRabbitMqAppQueueConsumer;
};

module.exports = { startRabbitMqConsumer };
