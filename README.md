rdkafka - Racket binding to Apache Kafka
=======

# TL;DR

* [Racket](https://racket-lang.org/) library to access[ Apache Kafka](https://kafka.apache.org/), using [librdkafka](https://github.com/edenhill/librdkafka).
* For now, just a low level access is provided (closely mapping the [C API of librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html)).
* Next step will be to provied a pure Scheme/Racket interface.

## How to run
* Assuming [you have Racket on your machine](https://download.racket-lang.org/), and have installed the following Racket packages:

```Shell
$ raco pkg install --skip-installed try-catch-finally uuid unix-signals threading

```

* Assuming `librdkafka` is [installed on your machine](https://github.com/edenhill/librdkafka#installation),

* Assuming there is a Kafka broker listening on `localhost:9092`, a `transactions` topic created with more than one partition (say 5), and [Kafka Connect Datagen ](https://github.com/confluentinc/kafka-connect-datagen) is installed, so that it's running the `transactions` [connector](https://github.com/confluentinc/kafka-connect-datagen/blob/master/config/connector_transactions.config) where `max.interval` is 5000 ms, then:

```Shell
$ racket complex-consumer.rkt -A -X partition.assignment.strategy=cooperative-sticky -g xoxo13 transactions
```

will start a consumer joining group `xoxo13` using the `COOPERATIVE` group protocol and reading from topic `transactions`. Starting in another window:

```Shell
$ racket complex-consumer.rkt -A -d protocol -g xoxo13 transactions
```
will attemp to start another customer in the `xoxo13` group, only failing since the default group protocol is `EAGER` and `xoxo13` is using the `COOPERATIVE` protocol for its group membership.
The `-d protocol` will cause debug info from the `protocol` category to be printed alongside.
Hitting `^C` (or sending `SINGINT` to the process) will cleanly shutdown the consumer. If you now restart it as:

```Shell
racket complex-consumer.rkt -A -X partition.assignment.strategy=cooperative-sticky -g xoxo13 transactions
```

your process will correctly join `xoxo13` group using a `COOPERATIVE` protocol, triggering a rebalancing of the group by the broker: after a while, each process will handle a subset of `transactions`' partition complement of the other's.


Note that the `-X` and `-d`  options can be repeated. For a list of all global parameters and their values:

```Shell
$ racket complex-consumer.rkt -X dump x
```

and:

```Shell
$ racket complex-consumer.rkt -h
usage: complex-consumer [ <option> ... ] <topic> [<topics>] ...
  For balanced consumer groups use the 'topic1 topic2..' format
  and for static assignment use 'topic1:part1 topic1:part2 topic2:part1..'

<option> is one of

  -g <g>
     Consumer group. ((null))
  -q
     Be quiet.
  -b <b>
     Broker address. (localhost:9092)
  -e
     Exit consumer when last message ∈ partition has been received.
  -D
     Describe group.
  -O
     Get committed offset(s).
  -A
     Raw payload output (consumer).
* -d <flag>
     set debug flag (all,generic,broker,topic...).
* -X <property>
     Set arbitrary librdkafka configuration property (name=value).
  --help, -h
     Show this help
  --
     Do not treat any remaining argument as a switch (at this level)

 *   Asterisks indicate options allowed multiple times.

 Multiple single-letter switches can be combined after
 one `-`. For example, `-h-` is the same as `-h --`.
```

## Caveat

1. It is currently **not possible** to set a `log` callback as doing so will cause a deadlock. `librdkafka` is heavily treaded and the log callback is called from all threads, often holding locks.  
	* hence `rd_kafka_conf_set_log_cb` is simply not exposed.
	* the workaround, is route all logging to a dedicated queue using `rd_kafka_set_log_queue` and set a thread to [periodically poll that queue](https://github.com/jsulmont/rdkafka/blob/main/complex-consumer.rkt#L300-L317).
* Ideally `librdkafka` library should be lifted to its own system thread (aka [place](https://docs.racket-lang.org/reference/places.html) in Racket parlance) and run under a dedicated [custodian](https://docs.racket-lang.org/reference/eval-model.html#%28part._custodian-model%29). Now I know, and this will probably happen at some time.
	
# Background

Lisp is totally cool and so is Kafka. Why not both?

## Learn Kafka Protocol

* Kafka uses a [wire protocol](https://kafka.apache.org/protocol) (binary/TCP) and defines all APIs as  sequences of requests/responses pairs. In other words, the only way to communicate with a Kafka broker, is to "speak" the protocol. Of course considering the complexity of problems solved by Kafka, this protocol isn't the simplest. Fortunately Apache Kafka releases official libraries as reference implementation of the Kafka Protocol (e.g., [kafka-clients](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients))
 
	
	* Clojurians using Kafka probably all know the excellent [jackdaw](https://github.com/FundingCircle/jackdaw), which amongst others leverages `kafka-clients`: [Clojure](https://clojure.org/) is yet another JVM language, and likewise all others (Scala, Kotlin, Groovy and ofc Java) can tap into the largest ecosystem ever. And of course since Clojure [*Clojure is a Lisp*](https://clojure.org/about/lisp) using Clojure/Jackdaw is indeed a very good way to do Kafka work -- that of course, if running JVMs is something you are happy with.

	* For sure the JVM is probably the most optimized and tested piece of software on earth, simply a wonder of technology. Yet the way it uses resources makes it unfit for a certain type of applications -- those for which resources are scarced and for whom small is beautiful.

	* Fortuanetly there is also [librdkafka](https://github.com/edenhill/librdkafka) which is a C implementation of Kafka Protocol: a library that C/C++ code that needs to interact with Kafka link against. On my MacOSX Intel running the Complex consumer example merely takes 2.2MB of RSS versus 212MB for a `kafka-console-consumer`.

# Motivation
