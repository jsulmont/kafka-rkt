A Racket interface to Apache Kafka
=======

# Motivation

* Lisp is totally cool and so is [Apache Kafka](https://kafka.apache.org/). Why not both? 
* [Racket](https://racket-lang.org/) is missing a package to access Apache Kafka.
  * [Clojurians](https://clojure.org/) have [jackdaw](https://github.com/FundingCircle/jackdaw), which leverages official Apache Kafka libraries.
  * [Common Lispers](https://en.wikipedia.org/wiki/Common_Lisp) have [cl-rdkafka](https://github.com/SahilKang/cl-rdkafka)


# Disclaimer
This project is a work in progress. Not to be used, or at your own risk.

# tl;dr

* A [Racket](https://racket-lang.org/) library to access[ Apache Kafka](https://kafka.apache.org/), using [librdkafka](https://github.com/edenhill/librdkafka).
   * For now, just a low level access is provided (closely mapping the [C API of librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html)). 
* A couple of examples, coverering 80% to 90% of `librdkafka`, almost a 1-1 mapping to their C counterpart. 
   * [infinite-retries-consumer.rkt](https://github.com/jsulmont/rkt-kafka/blob/main/infinite-retries-consumer.rkt) is a port of [retrieable-consumer](https://github.com/jsulmont/rkt-kafka/blob/main/infinite-retries-consumer.rkt).

* Next step will be a Racket idiomatic library, perhaps inspired by [jackdaw](https://github.com/FundingCircle/jackdaw) or [cl-kafka](https://github.com/SahilKang/cl-rdkafka).

## Caveat

* It is currently **not possible** to set a `log` callback as doing so will cause a deadlock. `librdkafka` is heavily treaded and the log callback is called from all threads, often holding locks.  
	* hence `rd_kafka_conf_set_log_cb` is simply not exposed.
	* the workaround, is route all logging to a dedicated queue using `rd_kafka_set_log_queue` and set a thread to [periodically poll that queue](https://github.com/jsulmont/rdkafka/blob/main/complex-consumer.rkt#L300-L317).
* It is currently not possible to run a stand-alone executable (created with `raco exe`). This is due to a problem with the [unix signals](https://github.com/tonyg/racket-unix-signals) library.

	


### Missing 

- [ ] Threading model (e.g., lift `librdkafka` to its own [place](https://docs.racket-lang.org/reference/places.html).
- [ ] Memory management (from malloc/free to GC, when to copy and when not etc).
- [ ] Exceptions 
- [ ] Basics (Toppar, Serde, Message)
- [ ] Admin API
- [ ] Producer API
- [ ] Consumer API
- [ ] Tests for above APIs
- [ ] Fix [unix signals](https://github.com/tonyg/racket-unix-signals) library.
- [ ] CI


## How to run the examples (will change)
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

