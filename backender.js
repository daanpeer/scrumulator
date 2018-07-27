const kafka = require("kafka-node");
const _ = require("lodash");
const crypto = require("crypto");

const options = {
  host: "zookeeper:2181",
  kafkaHost: "kafka:9092",
  groupId: "backenders",
  id: process.env.HOSTNAME,
  sessionTimeout: 15000,
  protocol: ["roundrobin"]
};

const consumerGroup = new kafka.ConsumerGroup(options, ["backend"]);

const client = new kafka.KafkaClient({
  kafkaHost: "kafka:9092"
});

const complexity = process.argv[2];

let isWorking = false;

const doWork = ({ storypoints, issue }) => {
  isWorking = true;
  console.log(
    "Working on story ",
    issue,
    " with a total of ",
    storypoints,
    " storypoints"
  );
  console.time("Working!");
  let working = true;
  let i = 0;
  while (working) {
    const id = crypto.randomBytes(20).toString("hex");
    const hash = crypto
      .createHash("sha1")
      .update(id)
      .digest("hex");

    // console.log(hash.substring(0, storypoints));
    if (hash.substring(0, storypoints) === "0".repeat(storypoints)) {
      console.log("done with hash!", hash);
      break;
    }

    i++;
  }
  console.timeEnd("Working!");
  isWorking = false;
};


const producer = new kafka.Producer(client);
producer.on('ready', () => {
  console.log(isWorking);
 
  // send a message that the work is complete
  producer.send(
    [{ topic: "lfw", partition: 0 }],
    (err, data) => {
      console.log(err, data);
    }
  ); 
});

console.log("Developer listening!");
consumerGroup.on("message", function(message, err) {
  console.log("Receiving work!");
  if (isWorking) {
    console.log("I'm already working!");
    return;
  }

  const work = JSON.parse(message.value);
  doWork(work);

  // send a message that the work is complete
  producer.send(
    [{ topic: "po", partition: 0, messages: JSON.stringify(work) }],
    (err, data) => {
      console.log(err, data);
    }
  );
});
