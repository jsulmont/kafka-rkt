#lang racket

(require racket/async-channel
         racket/string
         racket/match
         unix-signals
         try-catch-finally
         ffi/unsafe
         uuid
         kafka/rdkafka-ffi
         ; "ffi.rkt"
         )

(void
 (capture-signal! 'SIGINT))

(define (bytes->string bytes)
  (string-trim (bytes->string/latin-1 bytes) "\u0000" #:repeat? #t))

(define-values (delivered-ok delivered-nok) (values 0 0))

(define running? #t)

(define (stop)
  (set! running? #f))

(define (msg-delivery-cb client message opaque)
  (match (rd-kafka-message-err message)
    ['RD_KAFKA_RESP_ERR_NO_ERROR
     (displayln (format "Message delivered (~a bytes on topic ~a partition ~a at offset ~a)"
                        (rd-kafka-message-len message)
                        (rd-kafka-topic-name (rd-kafka-message-rkt message))
                        (rd-kafka-message-partition message)
                        (rd-kafka-message-offset message)))
     (set! delivered-ok (add1 delivered-ok))]
    [e (displayln (format "Message delivery failed: ~a" e))
       (set! delivered-nok (add1 delivered-nok))]))

(define (error-cb client err reason opaque)
  (displayln (format  "% Error: ~a: ~a" (rd-kafka-err2name err) reason))
  (when (equal? err 'RD_KAFKA_RESP_ERR__FATAL)
    (let* ([errstr-len 256]
           [errstr (make-bytes errstr-len)]
           [origin-error (rd-kafka-fatal-error client errstr errstr-len)])
      (displayln (format "% FATAL ERROR: ~a ~a"
                         (rd-kafka-err2name origin-error) errstr)))
    (displayln "% Terminating on fatal error")
    (stop)))

(define (set-conf conf key value errstr errstr-len)
  (let ([res (rd-kafka-conf-set conf key value errstr errstr-len)])
    (unless (eq? res 'RD_KAFKA_CONF_OK)
      (rd-kafka-conf-destroy conf)
      (raise (bytes->string errstr)))))

(define (make-conf pairs errstr errstr-len)
  (let ([conf (rd-kafka-conf-new)])
    (when (ptr-equal? conf #f)
      (raise "Failed to allocate conf"))
    (map (λ (pair) (set-conf conf (car pair) (cdr pair) errstr errstr-len)) pairs)
    conf))

(define (make-producer conf errstr errstr-len)
  (let ([producer (rd-kafka-new 'RD_KAFKA_PRODUCER conf errstr errstr-len)])
    (when (ptr-equal? producer #f)
      (rd-kafka-conf-destroy conf)
      (raise (bytes->string errstr)))
    producer))

(define (flush producer)
  (let ([err (rd-kafka-flush producer 5000)])
    (unless (eq? err 'RD_KAFKA_RESP_ERR_NO_ERROR)
      (if (eq? err 'RD_KAFKA_RESP_ERR__TIMED-OUT)
          (raise "Flush timed out!")
          (raise (format "Flush error!: `~A`" (rd-kafka-err2str err)))))))

(define (produce producer topic key value)
  (rd-kafka-producev
   producer
   'rd-kafka-vtype-topic
   topic
   'rd-kafka-vtype-key
   key
   (string-length key)
   'rd-kafka-vtype-value
   value
   (string-length value)
   'rd-kafka-vtype-msgflags
   RD-KAFKA-MESG-F-COPY
   'rd-kafka-vtype-end))

(define brokers (make-parameter "localhost:9092"))

(define topic
  (command-line
   #:program "idempotent-producer"
   #:once-each
   ["-b" b  "Bootstrap server" (brokers b)]
   #:args (topic) 
   topic))

(let* ([errstr-len 256]
       [errstr (make-bytes errstr-len)]
       [pairs (list (cons "bootstrap.servers"  (brokers))
                    (cons "enable.idempotence"  "true")
                #;("test.mock.num.brokers" . "3"))]
       [conf (make-conf pairs errstr (sub1 errstr-len))]
       [_ (rd-kafka-conf-set-dr-msg-cb conf msg-delivery-cb)]
       [_ (rd-kafka-conf-set-error-cb conf error-cb)]
       [client (make-producer conf errstr errstr-len)]
       [msgcnt 0]
       [signal-thunk
        (thread (λ () (let loop ()
                        (define signum (read-signal))
                        (printf "Received signal ~v (name ~v)\n" signum (lookup-signal-name signum))
                        (stop)
                        (loop))))])
  (let loop ()
    (when running?
      (let* ([key (uuid-string)]
             [msg (format "uuid ~a produced at ~a ms" key
                          (current-inexact-monotonic-milliseconds))])

        (let retry ([err (produce client topic key msg)])
          (cond
            [(equal? err 'RD_KAFKA_RESP_ERR__QUEUE_FULL)
             (begin ;; we retry
               (rd-kafka-poll client 1000)
               (retry (produce client topic key msg)))]
            [(not (equal? err 'RD_KAFKA_RESP_ERR_NO_ERROR))
             (begin ;; bail out
               (stop)
               (loop))])))
      (rd-kafka-poll client 0)
      (when (equal? msgcnt 13)
        (rd-kafka-test-fatal-error
         client
         'RD_KAFKA_RESP_ERR_OUT_OF_ORDER_SEQUENCE_NUMBER
         "This is a fabricated error to test the fatal error handling"))

      (set! msgcnt (add1 msgcnt))

      (sleep 0.8)

      (when running? (loop))))

  (displayln "% Flushing outstanding messages ...")

  (flush client)

  (displayln (format "% ~a messages(s) produced, ~a delivered, ~a failed"
                     msgcnt  delivered-ok delivered-nok))

  (let ([err (rd-kafka-fatal-error client #f 0)])
    (rd-kafka-destroy client)
    (unless (eq? err  'RD_KAFKA_RESP_ERR__NO_ERROR)
      (exit 1))
    (exit 0))) 
