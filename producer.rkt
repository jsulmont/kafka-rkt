#lang racket/base

(require racket/system
         racket/string
         racket/match
         try-catch-finally
         ffi/unsafe
         uuid
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
    (unless (eq? res 'conf-ok)
      (rd-kafka-conf-destroy conf)
      (raise (bytes->string errstr)))))

(define (make-conf pairs errstr errstr-len)
  (let ([conf (rd-kafka-conf-new)])
    (when (ptr-equal? conf #f)
      (raise "Failed to allocate conf"))
    (map (λ (pair) (set-conf conf (car pair) (cdr pair) errstr errstr-len)) pairs)
    (rd-kafka-conf-set-dr-msg-cb conf msg-delivery-cb)
    conf))

(define (make-producer conf errstr errstr-len)
  (let ([producer (rd-kafka-new 'producer conf errstr errstr-len)])
    (when (ptr-equal? producer #f)
      (rd-kafka-conf-destroy conf)
      (raise (bytes->string errstr)))
    producer))

(define (produce producer topic key value)
  (try (unless (and (string? key) (string? value))
         (raise (format "Error: both key and value must be strings")))
       (let ([err (rd-kafka-producev producer
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
                                     'rd-kafka-vtype-end)])
         (unless (eq? err 'RD_KAFKA_RESP_ERR_NO_ERROR)
           (raise (format "Failed to produce message: `~A`" (rd-kafka-err2str err)))))
       (finally (rd-kafka-poll producer 0))))

(define (consume-messages bootstrap-servers topic)
  (let ([cmd (format "gtimeout 5 kcat -Ce -b '~A' -t '~A' || exit 0" bootstrap-servers topic)])
    (system/exit-code cmd)))

(define (flush producer)
  (let ([err (rd-kafka-flush producer 5000)])
    (unless (eq? err 'RD_KAFKA_RESP_ERR_NO_ERROR)
      (if (eq? err 'RD_KAFKA_RESP_ERR__TIMED-OUT)
          (raise "Flush timed out!")
          (raise (format "Flush error!: `~A`" (rd-kafka-err2str err)))))))

(let* ([errstr-len 256]
       [errstr (make-bytes errstr-len)]
       [pairs '(("bootstrap.servers" . "localhost:9092") ("enable.idempotence" . "true")
                                                         #;("test.mock.num.brokers" . "3"))]
       [conf (make-conf pairs errstr (sub1 errstr-len))]
       [producer (make-producer conf errstr (sub1 errstr-len))])
  (try
   (let loop ([i 0])
     (if (equal? i 10000)
         'DONE
         (let ([key (uuid-string)])
           (produce producer
                    "foobar"
                    key
                    (format "uuid ~a produced at ~a ms" key (current-inexact-monotonic-milliseconds)))
           (sleep 0.5)
           (loop (add1 i)))))
   (catch (exn:fail? e) (displayln (format "bummer! got ~a" e)))
   (finally (rd-kafka-destroy producer))))
