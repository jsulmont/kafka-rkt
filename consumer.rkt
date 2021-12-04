#lang racket/base

(require racket/string
         racket/match
         ffi/unsafe
         "ffi.rkt")

(define (bytes->string bytes)
  (string-trim (bytes->string/latin-1 bytes) "\u0000" #:repeat? #t))

(define (msg-delivery-cb client message opaque)
  (match (rd-kafka-message-err message)
    ['RD_KAFKA_RESP_ERR_NO_ERROR (displayln (format "Message delivered (~a bytes on partition ~a)"
                                                    (rd-kafka-message-len message)
                                                    (rd-kafka-message-partition message)))]
    [e (displayln (format "Message delivery failed: ~a" e))]))

(define (set-conf conf key value errstr errstr-len)
  (let ([res (rd-kafka-conf-set conf key value errstr errstr-len)])
    (unless (eq? res 'RD_KAFKA_CONF_OK)
      (raise (bytes->string errstr)))))

(define (make-conf pairs errstr errstr-len)
  (let ([conf (rd-kafka-conf-new)])
    (when (ptr-equal? conf #f)
      (raise "Failed to allocate conf"))
    (map (λ (pair) (set-conf conf (car pair) (cdr pair) errstr errstr-len)) pairs)
    conf))

(define (make-consumer conf errstr errstr-len)
  (let ([producer (rd-kafka-new 'RD_KAFKA_CONSUMER conf errstr errstr-len)])
    (when (ptr-equal? producer #f)
      (raise (bytes->string errstr)))
    producer))

(define (subscribe consumer . topics)
  (let* ([subscription (rd-kafka-topic-partition-list-new (length topics))]
         [_ (rd-kafka-poll-set-consumer consumer)]
         [_ (map (λ (topic)
                   (rd-kafka-topic-partition-list-add subscription topic RD_KAFKA_PARTITION_UA))
                 topics)]
         [err (rd-kafka-subscribe consumer subscription)])

    (unless (eq? err 'RD_KAFKA_RESP_ERR_NO_ERROR)
      (raise (format "subscribe failed: ~a" (rd-kafka-err2str err))))

    (displayln (format "%% Subscribed to ~a topic(s), waiting for rebalance and messages ..."
                       (rd-kafka-topic-partition-list-cnt subscription)))))

(let* ([errstr-len 128]
       [errstr (make-bytes errstr-len)]
       [pairs '(("bootstrap.servers" . "localhost:9092") ("group.id" . "rdk-consxx-71aaad134")
                                                         ("enable.auto.commit" . "true")
                                                         ("auto.offset.reset" . "latest"))]
       [conf (make-conf pairs errstr errstr-len)]
       [consumer (make-consumer conf errstr errstr-len)])

  (subscribe consumer "foobar" "stock-trades" "credit_cards" "transactions")

  (let loop ([ptr (rd-kafka-consumer-poll consumer 100)])
    (unless ptr
      (loop (rd-kafka-consumer-poll consumer 100)))

    (let* ([msg (ptr-ref ptr _rd-kafka-message)]
           [err (rd-kafka-message-err msg)])

      (unless (eq? err 'RD_KAFKA_RESP_ERR_NO_ERROR)
        (displayln (format "%% Consumer error: ~a" (rd-kafka-err2str err)))
        (rd-kafka-message-destroy ptr)
        (loop (rd-kafka-consumer-poll consumer 100)))

      (let* ([msgstr (format "Message on ~a [~a] at offset ~a"
                             (rd-kafka-topic-name (rd-kafka-message-rkt msg))
                             (rd-kafka-message-partition msg)
                             (rd-kafka-message-offset msg))]
             [key-len (rd-kafka-message-key-len msg)]
             [keystr (if (equal? key-len 0) "no key" (format "~a bytes key" key-len))]
             [len (rd-kafka-message-len msg)]
             [valstr (if (equal? len 0) "no value" (format "~a bytes value" len))])
        (displayln (format "~a: ~a, ~a" msgstr keystr valstr)))
      (rd-kafka-message-destroy ptr)
      (loop (rd-kafka-consumer-poll consumer 1000)))))
