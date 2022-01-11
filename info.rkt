#lang info
(define collection "kafka")
(define deps '("base"))
(define build-deps '("scribble-lib" "racket-doc" "rackunit-lib"))
(define scribblings '(("scribblings/kafka.scrbl" ())))
(define pkg-desc "A Racket Kafka client built on top of librdkafka")
(define version "0.1")
(define pkg-authors '(jsulmont))
(define license '(Apache-2.0 OR MIT))
