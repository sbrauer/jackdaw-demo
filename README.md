# jackdaw demo

This is a demo of [jackdaw](https://github.com/FundingCircle/jackdaw) that shows how to create a Streams app that uses the Avro Schema Registry and does a simple aggregation.


## Setting up

Before starting, it is recommended to download and install the
Confluent CLI which can be obtained from
[https://www.confluent.io/download/](https://www.confluent.io/download/).

Add the Confluent `bin` directory to your `PATH`.
For example:
```
export PATH=~/Downloads/confluent-5.2.1/bin:$PATH
```

To install Clojure:
[https://clojure.org/guides/getting_started](https://clojure.org/guides/getting_started).

## Running

### Run the app from shell/commandline

Start the confluent platform (kafka and friends):
```
confluent start
```

Start the stream demo app:
```
clj --main demo
```

Watch the output topic:
```
kafka-avro-console-consumer --bootstrap-server localhost:9092 --property print.key=true --topic items
```

Publish to the input topic:
```
kafka-avro-console-producer --broker-list localhost:9092 --topic commands --property parse.key=true --property value.schema="{\"type\":\"record\",\"name\":\"Demo\",\"fields\":[{\"name\":\"op\",\"type\":{\"type\":\"enum\",\"name\":\"Operator\",\"symbols\":[\"add\",\"del\"]}},{\"name\":\"item\",\"type\":\"string\"}]}" --property key.schema='"string"' --property schema.registry.url=http://localhost:8081
```
At this point you can enter messages (KV pairs) one line at a time.
Be sure to separate the key and value with a single Tab character.
Example records:
```
"test"   {"op":"add","item":"foo"}
"test"   {"op":"add","item":"bar"}
"test"   {"op":"add","item":"baz"}
"test"   {"op":"del","item":"bar"}
"test2"   {"op":"add","item":"Hello"}
"test2"   {"op":"add","item":"World"}
```

### Run the app via the Clojure repl in cider (or similar)

Open `src/demo.clj` in your editor (emacs) and start a repl.
Evaluate form from the `comment` block at the bottom of the file.

