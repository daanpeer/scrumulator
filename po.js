const kafka = require("kafka-node");
const _ = require("lodash");

const client = new kafka.KafkaClient({
  kafkaHost: "kafka:9092",
  // partitionerType: 0
});

const producer = new kafka.Producer(client);

client.once("connect", function() {
  client.loadMetadataForTopics([], function(error, results) {
    if (error) {
      return console.error(error);
    }
    console.log("%j", _.get(results, "1.metadata"));
  });
});

producer.on("ready", () => {
  console.log("ready!");

  setInterval(() => {
    producer.send(
      [
        { topic: "backend", partition: 0, messages: "appel" },
        { topic: "backend", partition: 1, messages: "tomaat" }
      ],
      (err, data) => {
        console.log(err, data);
      }
    );
  }, 1000);

  // producer.send(payloads, function(err, data) {
  //   console.log(data);
  // });
});
