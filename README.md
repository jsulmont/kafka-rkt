rdkafka - Racket binding to Apache Kafka
=======


# Rationale

Lisp is totally cool and so is Kafka. Why not both?

* Kafka uses a [wire protocol](https://kafka.apache.org/protocol) (binary/TCP) and defines all APIs as  sequences of requests/responses pairs. In other words, the only way to "talk" to a Kafka broker is to "speak" the protocol. Of course considering the complexity of problems solved by Kafka, this protocol isn't the simplest. Fortunately Apache Kafka releases official libraries as reference implementation of the Kafka Protocol (e.g., [kafka-clients](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients))

* Clojurians using Kafka probably all know the excellent [jackdaw](https://github.com/FundingCircle/jackdaw), which amongst others leverages `kafka-clients`: [Clojure](https://clojure.org/) is yet another JVM language, and likewise all others (Scala, Kotlin, Groovy and ofc Java) can tap into the largest ecosystem ever.

* For sure the JVM is probably the most optimized and tested piece of software on the earth, and is a wonder of technology. Yet the way it uses resources makes it unfit for a certain type of applications -- those for which resources are scarced and for whom small is beautiful.

* Fortuanetly there is also [librdkafka](https://github.com/edenhill/librdkafka) which is a C implementation of Kafka Protocol: a library that C/C++ code that needs to interact with Kafka link against. On my MacOSX Intel running the complex consumer example merely takes 2.2MB of RSS versus 212MB for a `kafka-console-consumer`.