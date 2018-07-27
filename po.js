const kafka = require("kafka-node");
const _ = require("lodash");

const client = new kafka.KafkaClient({
  kafkaHost: "kafka:9092",
});

const producer = new kafka.Producer(client);

const sprintBacklog = {
  todo: [
    {
      id: 1,
      issue: "[ISSUE-123] - As developer I want to make cool stuff",
      storypoints: 5,
    },
    {
      id: 2,
      issue: "[ISSUE-124] - As po I want cool stuff to be made",
      storypoints: 5,
    },
    {
      id: 3,
      issue: "[ISSUE-125] - As test123 I want cool stuff to be made",
      storypoints: 5,
    }
  ],
  doing: [],
  done: [],
};

/*
sprint planning:
* throw event on the kafka stream
* wait for developers to be present (msg)
* if developers are present do the planning itself
* deliverable is a sprint backlog

sprint:
* a sprint consist of x "ticks"
* pop work from the sprint backlog
* throw the work on the kafka stream
* let the developers perform work

work:
* developer asks for work
* the developer can receive work
* notifies that the work is in progress
* the developer notifies PO when work is complete
* the board is updated
*/

const wait = (ms) => new Promise((resolve) => {
  setTimeout(() => {
    resolve();
  }, ms);
})

producer.on("ready", async () => {
  console.log("ready!");

  producer.send(sprintBacklog.todo.map((b, index) => ({
    topic: "backend",
    partition: index,
    messages: JSON.stringify(b),
  })), (err, data) => {
    console.log(err, data);
  });
  
});

const options = {
  host: 'zookeeper:2181',
  kafkaHost: 'kafka:9092',
  groupId: 'backenders',
  id: process.env.HOSTNAME,
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
};
const consumerGroup = new kafka.ConsumerGroup(options, ['po'])
consumerGroup.on("message", function(message, err) {
  console.log('received message');
  const msg = JSON.parse(message.value);
  console.log('po received', msg);
  // console.log('received message!', message);
});
