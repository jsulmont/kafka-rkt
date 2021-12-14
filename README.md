rdkafka - Racket binding to Apache Kafka
=======

# TL;DR

* [Racket](https://racket-lang.org/) library to access[ Apache Kafka](https://kafka.apache.org/), using [librdkafka](https://github.com/edenhill/librdkafka).
* For now, just a low level access is provided (closely mapping the [C API of librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html)).
* Next step will be to provied a pure Scheme/Racket interface.

# How to run
* Assuming you have [installed racket](https://download.racket-lang.org/) on your machine, and have installed the `try-catch-finally` moduled as following:

```Shell
$ raco pkg install --skip-installed try-catch-finally uuid unix-signals threading

```

* Assuming you a Kafka broker listening to `localhost:9092`, a `transactions` topic created with more than one partition (say 5), and a [Kafka Connect Datagen ](https://github.com/confluentinc/kafka-connect-datagen) installed, so that it's running the `transactions` [connector](https://github.com/confluentinc/kafka-connect-datagen/blob/master/config/connector_transactions.config) where `max.interval` is 500 ms, then:

```Shell
$ racket complex-consumer.rkt -A -X partition.assignment.strategy=cooperative-sticky -g xoxo13 transactions
```

will start a consumer joining group `xoxo13` using the `cooperative-sticky` group protocol and reading from topic `transactions`,

and in another window:

```Shell
$ racket complex-consumer.rkt -A -d protocol -g xoxo13 transactions
```
will, printing alongside debug infos from the `protocol` group, will attemp to start another customer in the `xoxo13` group, only failing since the default group protocol is `EAGER` and `xoxo13` is uses the `cooperative-sticky` protocol for its membership.
Hitting `Control C` (or sending `SINGINT`) to the process will cleanly shutdown the consumer.
Note that the `-X` option can be repeated as needed. For a list of all global parameters and their values:

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

# Rationale

Lisp is totally cool and so is Kafka. Why not both?

* Kafka uses a [wire protocol](https://kafka.apache.org/protocol) (binary/TCP) and defines all APIs as  sequences of requests/responses pairs. In other words, the only way to "talk" to a Kafka broker is to "speak" the protocol. Of course considering the complexity of problems solved by Kafka, this protocol isn't the simplest. Fortunately Apache Kafka releases official libraries as reference implementation of the Kafka Protocol (e.g., [kafka-clients](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients))

* Clojurians using Kafka probably all know the excellent [jackdaw](https://github.com/FundingCircle/jackdaw), which amongst others leverages `kafka-clients`: [Clojure](https://clojure.org/) is yet another JVM language, and likewise all others (Scala, Kotlin, Groovy and ofc Java) can tap into the largest ecosystem ever.

* For sure the JVM is probably the most optimized and tested piece of software on the earth, and is a wonder of technology. Yet the way it uses resources makes it unfit for a certain type of applications -- those for which resources are scarced and for whom small is beautiful.

* Fortuanetly there is also [librdkafka](https://github.com/edenhill/librdkafka) which is a C implementation of Kafka Protocol: a library that C/C++ code that needs to interact with Kafka link against. On my MacOSX Intel running the complex consumer example merely takes 2.2MB of RSS versus 212MB for a `kafka-console-consumer`.

* Blah blah blah