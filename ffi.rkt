#lang racket/base

(require racket/list
         racket/match
         threading
         setup/dirs
         ffi/unsafe
         ;         ffi/unsafe/alloc
         ffi/unsafe/define
         ffi/unsafe/define/conventions)

(define rdkafka-lib
  (ffi-lib "librdkafka"
           '("1" #f)
           #:get-lib-dirs
           (λ ()
             (cons (string->path "/usr/local/Cellar/librdkafka/1.8.2/lib/")
                   (get-lib-search-dirs)))))

(define-ffi-definer define-rdkafka rdkafka-lib #:make-c-id convention:hyphen->underscore)

;;; ---------------------------------
;;; @name librdkafka version
;;; ---------------------------------

(define RD_KAFKA_VERSION #x010802ff)

(define-rdkafka rd-kafka-version (_fun -> _int))

(define-rdkafka rd-kafka-version-str (_fun -> _string))

(provide RD_KAFKA_VERSION
         rd-kafka-version
         rd-kafka-version-str)

;;; ---------------------------------
;;; @name Constants, errors, types
;;; ---------------------------------

(define _rd-kafka-type (_enum '(RD_KAFKA_PRODUCER RD_KAFKA_CONSUMER)))

(define _rd-kafka-timestamp-type
  (_enum '(RD_KAFKA_TIMESTAMP_NOT_AVAILABLE RD_KAFKA_TIMESTAMP_CREATE_TIME
                                            RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME)))

;;; bunch of opaque types
(define _rd-kafka-pointer (_cpointer 'rd-kafka))
(define _rd-kafka-topic-pointer (_cpointer 'rd-kafka-topic))
(define _rd-kafka-conf-pointer (_cpointer 'rd-kafka-conf))
(define _rd-kafka-topic-conf-pointer (_cpointer 'rd-kafka-topic-conf))
(define _rd-kafka-queue-pointer (_cpointer 'rd-kafka-queue))
(define _rd-kafka-event-pointer (_cpointer 'rd-kafka-event))
(define _rd-kafka-topic-result-pointer (_cpointer 'rd-kafka-topic-result))
(define _rd-kafka-consumer-group-metadata-pointer (_cpointer 'rd-kafka-consumer-group-metadata))
(define _rd-kafka-error-pointer (_cpointer 'rd-kafka-error))
(define _rd-kafka-headers-pointer (_cpointer 'rd-kafka-headers))
(define _rd-kafka-group-result-pointer (_cpointer 'rd-kafka-group-result))

(define-rdkafka rd-kafka-get-debug-contexts (_fun -> _string))

;;; ERRORS
(define-cstruct _rd-kafka-err-desc ([code _int] [name _string] [desc _string]))

(define _rd-kafka-resp-err
  (let* ([get-err-descs
          (get-ffi-obj "rd_kafka_get_err_descs"
                       rdkafka-lib
                       (_fun (ps : (_ptr o _rd-kafka-err-desc-pointer))
                             (n : (_ptr o _size))
                             ->  _void
                             ->  (values ps n)))]
         [err-codes
          (~>> (flatten (let-values ([(x y) (get-err-descs)])
                          (for/list ([i (in-range y)])
                            (match (ptr-ref x (_list-struct _int _string _string) i)
                              [(list code name _)
                               #:when name
                               (list (string->symbol (format "RD_KAFKA_RESP_ERR_~A" name)) '= code)]
                              [(list _ #f _) '()]))))
               (filter-not empty?))])
    (_enum err-codes _fixint)))

(define-rdkafka rd-kafka-err2str (_fun _rd-kafka-resp-err -> _string))

(define-rdkafka rd-kafka-err2name (_fun _rd-kafka-resp-err -> _string))

(define-rdkafka rd-kafka-last-error (_fun -> _rd-kafka-resp-err))

(define-rdkafka rd-kafka-fatal-error (_fun _rd-kafka-pointer _pointer _int -> _rd-kafka-resp-err))

(define-rdkafka rd-kafka-error-code (_fun _rd-kafka-error-pointer -> _rd-kafka-resp-err))

(define-rdkafka rd-kafka-error-name (_fun _rd-kafka-error-pointer -> _string))

(define-rdkafka rd-kafka-error-string (_fun _rd-kafka-error-pointer -> _string))

(define-rdkafka rd-kafka-error-is-fatal (_fun _rd-kafka-error-pointer -> _stdbool))

(define-rdkafka rd-kafka-error-is-retriable (_fun _rd-kafka-error-pointer -> _stdbool))

(define-rdkafka rd-kafka-error-txn-requires-abort (_fun _rd-kafka-error-pointer -> _stdbool))

(provide rd-kafka-err2str
         rd-kafka-err2name
         rd-kafka-last-error
         rd-kafka-fatal-error
         rd-kafka-error-code
         rd-kafka-error-name
         rd-kafka-error-string
         rd-kafka-error-is-fatal
         rd-kafka-error-is-retriable
         rd-kafka-error-txn-requires-abort)

;;; ---------------------------------
;;; @name Kafka messages
;;; ---------------------------------

(define-cstruct _rd-kafka-message
                ([err _rd-kafka-resp-err] [rkt _rd-kafka-topic-pointer]
                                          [partition _int32]
                                          [payload _pointer]
                                          [len _size]
                                          [key _pointer]
                                          [key_len _size]
                                          [offset _int64]
                                          (private _pointer)))

(define-rdkafka rd-kafka-message-destroy (_fun _rd-kafka-message-pointer -> _void))

(define-rdkafka rd-kafka-message-errstr (_fun _rd-kafka-message-pointer -> _string))

(define-rdkafka rd-kafka-message-timestamp (_fun _rd-kafka-message-pointer -> _int64))

(define-rdkafka rd-kafka-message-latency (_fun _rd-kafka-message-pointer -> _int64))

(define-rdkafka rd-kafka-message-broker-id (_fun _rd-kafka-message-pointer -> _int32))

(define-rdkafka rd-kafka-message-headers
                (_fun _rd-kafka-message-pointer _pointer -> _rd-kafka-resp-err))

(define-rdkafka rd-kafka-message-detach-headers
                (_fun _rd-kafka-message-pointer _pointer -> _rd-kafka-resp-err))

(define _rd-kafka-msg-status
  (_enum '(RD_KAFKA_MSG_STATUS_NOT_PERSISTED RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED
                                             RD_KAFKA_MSG_STATUS_PERSISTED)))

(define-rdkafka rd-kafka-message-status (_fun _rd-kafka-message-pointer -> _rd-kafka-msg-status))

(provide _rd-kafka-message
         (struct-out rd-kafka-message)
         rd-kafka-message-destroy
         rd-kafka-message-errstr
         rd-kafka-message-timestamp
         rd-kafka-message-latency
         rd-kafka-message-broker-id
         rd-kafka-message-headers
         rd-kafka-message-detach-headers
         rd-kafka-message-status)

;;; ---------------------------------
;;; @name configuration interface
;;; ---------------------------------

;; TODO check wrap for both next
(define-rdkafka rd-kafka-conf-destroy (_fun _rd-kafka-conf-pointer -> _void))

(define-rdkafka rd-kafka-conf-new (_fun -> _rd-kafka-conf-pointer))

(define _rd-kafka-conf-res
  (_enum '(RD_KAFKA_CONF_UNKNOWN = -2 RD_KAFKA_CONF_INVALID = -1 RD_KAFKA_CONF_OK = 0) _fixint))

(define-rdkafka rd-kafka-conf-properties-show (_fun _pointer -> _void))

(define-rdkafka rd-kafka-conf-set
                (_fun _rd-kafka-conf-pointer _string _string _bytes _size -> _rd-kafka-conf-res))

(define-rdkafka rd-kafka-conf-dup (_fun _rd-kafka-conf-pointer -> _rd-kafka-conf-pointer))

(define-rdkafka rd-kafka-conf (_fun _rd-kafka-pointer -> _rd-kafka-conf-pointer))

(define-rdkafka rd-kafka-conf-set-events (_fun _rd-kafka-conf-pointer _int -> _void))

(define _dr-msg-cb (_fun _rd-kafka-pointer _rd-kafka-message-pointer _pointer -> _void))

(define-rdkafka rd-kafka-conf-set-dr-msg-cb (_fun _rd-kafka-conf-pointer _dr-msg-cb -> _void))

(define _background-event-cb (_fun _rd-kafka-pointer _rd-kafka-event-pointer _pointer -> _void))

(define-rdkafka rd-kafka-conf-set-background-event-cb
                (_fun _rd-kafka-conf-pointer _background-event-cb -> _void))

(define-rdkafka rd-kafka-conf-dump-free (_fun [arr : _pointer] [cnt : _size] -> _void))

(define-rdkafka rd-kafka-conf-dump
  (_fun _rd-kafka-conf-pointer
        [cnt : (_ptr o _size)]
        -> [arr : _pointer]
        -> (let ([lst (cblock->list arr _string cnt)])
             (rd-kafka-conf-dump-free arr cnt)
             lst)))

(define-rdkafka rd-kafka-topic-conf-dump
  (_fun _rd-kafka-topic-conf-pointer
        [cnt : (_ptr o _size)]
        -> [arr : _pointer]
        ->  (let ([lst (cblock->list arr _string cnt)])
              (rd-kafka-conf-dump-free arr cnt)
              lst)))

(define-rdkafka rd-kafka-conf-set-default-topic-conf
  (_fun _rd-kafka-conf-pointer _rd-kafka-topic-conf-pointer -> _void))

(define-rdkafka rd-kafka-conf-get-default-topic-conf
  (_fun _rd-kafka-conf-pointer -> _rd-kafka-topic-conf-pointer))

(provide
 rd-kafka-conf-properties-show
 rd-kafka-get-debug-contexts
 rd-kafka-conf-destroy
 rd-kafka-conf-set
 rd-kafka-conf-new
 rd-kafka-conf-dup
 rd-kafka-conf
 rd-kafka-conf-set-events
 _dr-msg-cb
 _background-event-cb
 rd-kafka-conf-set-dr-msg-cb
 rd-kafka-conf-set-background-event-cb
 rd-kafka-conf-dump
 rd-kafka-topic-conf-dump
 rd-kafka-conf-set-default-topic-conf
 rd-kafka-conf-get-default-topic-conf)

;;; ---------------------------------
;;; @name Kafka main object
;;; ---------------------------------

(define-rdkafka
  rd-kafka-destroy (_fun _rd-kafka-pointer -> _void))

(define-rdkafka rd-kafka-new
  (_fun _rd-kafka-type _rd-kafka-conf-pointer _bytes _size -> _rd-kafka-pointer))

(provide
 rd-kafka-new
 rd-kafka-destroy)

(define RD-KAFKA-MESG-F-FREE #x1)
(define RD-KAFKA-MESG-F-COPY #x2)
(define RD-KAFKA-MESG-F-BLOCK #x4)
(define RD-KAFKA-MESG-F-PARTITION #x8)

(define-rdkafka
 rd-kafka-produce
 (_fun _rd-kafka-pointer _int32 _int _pointer _size _pointer _size _pointer -> _rd-kafka-resp-err))

(define rd-kafka-vtypes
  '(rd-kafka-vtype-end
    rd-kafka-vtype-topic
    rd-kafka-vtype-rkt
    rd-kafka-vtype-partition
    rd-kafka-vtype-value
    rd-kafka-vtype-key
    rd-kafka-vtype-opaque
    rd-kafka-vtype-msgflags
    rd-kafka-vtype-timestamp
    rd-kafka-vtype-header
    rd-kafka-vtype-headers))

(define _rd-kafka-vtype (_enum rd-kafka-vtypes))

;;; TODO use `memoize`
(define producev-interfaces (make-hash))
(define (rd-kafka-producev producer . args)
  (define itypes
    (cons _pointer
          (map (λ (x)
                 (cond
                   [(member x rd-kafka-vtypes) _rd-kafka-vtype]
                   [(and (integer? x) (exact? x)) _int]
                   [(and (number? x) (real? x)) _double*]
                   [(string? x) _string]
                   [(bytes? x) _bytes]
                   [(symbol? x) _symbol] ;; TODO add type for opaque
                   [else (error 'rd-kafka-producev "don't know how to deal with ~e" x)]))
               args)))
  (let ([producev (hash-ref producev-interfaces
                            itypes
                            (λ ()
                              (let ([i (get-ffi-obj "rd_kafka_producev"
                                                    rdkafka-lib
                                                    (_cprocedure itypes _rd-kafka-resp-err))])
                                (hash-set! producev-interfaces itypes i)
                                i)))])
    (apply producev (cons producer args))))

(define-rdkafka rd-kafka-produce-batch
  (_fun _rd-kafka-pointer _int32 _int _pointer _size -> _size))

(define-rdkafka rd-kafka-poll
  (_fun _rd-kafka-pointer _int -> _int))

(define-rdkafka rd-kafka-outq-len
  (_fun _rd-kafka-pointer -> _int))

(define-rdkafka rd-kafka-flush
  (_fun _rd-kafka-pointer _int -> _rd-kafka-resp-err))

(provide
 RD-KAFKA-MESG-F-FREE
 RD-KAFKA-MESG-F-COPY
 RD-KAFKA-MESG-F-BLOCK
 RD-KAFKA-MESG-F-PARTITION
 rd-kafka-produce
 rd-kafka-producev
 rd-kafka-produce-batch
 rd-kafka-poll
 rd-kafka-outq-len
 rd-kafka-flush)

(define-cstruct _rd-kafka-topic-partition
  ([topic _string]
   [partition _int32]
   [offset _int64]
   [metadata _pointer]
   [metadata-size _size]
   [opaque _size]
   [err _rd-kafka-resp-err]
   (private _pointer)))

(define-cstruct _rd-kafka-topic-partition-list
  ([cnt _int]
   [size _int]
   [elems _rd-kafka-topic-partition-pointer]))

(define-rdkafka rd-kafka-commit
  (_fun _rd-kafka-pointer -> _rd-kafka-resp-err))

(define-rdkafka rd-kafka-topic-partition-list-destroy
  (_fun _rd-kafka-topic-partition-list-pointer -> _void))

(define-rdkafka rd-kafka-topic-partition-list-new
  (_fun _int -> _rd-kafka-topic-partition-list-pointer))

(define-rdkafka rd-kafka-topic-partition-list-add
  (_fun _rd-kafka-topic-partition-list-pointer _string _int32 -> _rd-kafka-topic-partition-pointer))

(define-rdkafka rd-kafka-topic-partition-list-add-range
  (_fun _rd-kafka-topic-partition-list-pointer _string _int32 _int32 -> _void))

(define-rdkafka rd-kafka-topic-partition-list-del
  (_fun _rd-kafka-topic-partition-list-pointer _string _int32 -> _int))

(define-rdkafka rd-kafka-topic-partition-list-del-by-idx
  (_fun _rd-kafka-topic-partition-list-pointer _int32 -> _int))

(define-rdkafka rd-kafka-topic-partition-list-copy
  (_fun _rd-kafka-topic-partition-list-pointer -> _rd-kafka-topic-partition-list-pointer))

(define-rdkafka rd-kafka-topic-partition-list-set-offset
  (_fun _rd-kafka-topic-partition-list-pointer _string _int32 _int64 -> _rd-kafka-resp-err))

(provide
 _rd-kafka-topic-partition
 (struct-out rd-kafka-topic-partition)
 _rd-kafka-topic-partition-list
 (struct-out rd-kafka-topic-partition-list)
 rd-kafka-commit
 rd-kafka-topic-partition-list-destroy
 rd-kafka-topic-partition-list-new
 rd-kafka-topic-partition-list-add
 rd-kafka-topic-partition-list-add-range
 rd-kafka-topic-partition-list-del
 rd-kafka-topic-partition-list-del-by-idx
 rd-kafka-topic-partition-list-copy
 rd-kafka-topic-partition-list-set-offset)

(define-rdkafka rd-kafka-poll-set-consumer
  (_fun _rd-kafka-pointer -> _rd-kafka-resp-err))

(define RD_KAFKA_PARTITION_UA -1)

(define-rdkafka rd-kafka-subscribe
  (_fun _rd-kafka-pointer _rd-kafka-topic-partition-list-pointer -> _rd-kafka-resp-err))

(define-rdkafka rd-kafka-consumer-poll
  (_fun _rd-kafka-pointer _int -> _rd-kafka-message-pointer/null))

(define-rdkafka rd-kafka-consumer-close
  (_fun _rd-kafka-pointer _int -> _rd-kafka-message-pointer))

(provide
 rd-kafka-poll-set-consumer
 RD_KAFKA_PARTITION_UA
 rd-kafka-subscribe
 rd-kafka-consumer-poll
 rd-kafka-consumer-close)

;;;; TOPICS
(define-rdkafka rd-kafka-topic-name (_fun _rd-kafka-topic-pointer -> _string))

(provide rd-kafka-topic-name)

;;; ---------------------------------
;;; @name Metadata API
;;; ---------------------------------

(define-cstruct _rd-kafka-metadata-broker
  ([id _int32]
   [host _string]
   [port _int]))

(define-cstruct _rd-kafka-metadata-partition
  ([id _int32]
   [err _rd-kafka-resp-err]
   [leader _int32]
   [replica-cnt _int]
   [replicas _pointer]
   [isr-cnt _int]
   [isrs _pointer]))

(define-cstruct _rd-kafka-metadata-topic
  ([topic _string]
   [partition-cnt _int]
   [partitions _rd-kafka-metadata-partition-pointer]
   [err _rd-kafka-resp-err]))

(define-cstruct _rd-kafka-metadata
  ([broker-cnt _int]
   [brokers _rd-kafka-metadata-broker-pointer]
   [topic-cnt _int]
   [topics _rd-kafka-metadata-topic-pointer]
   [origin-broker-id _int32]
   [origin-broker-name _string]))

(provide
 _rd-kafka-metadata-broker
 (struct-out rd-kafka-metadata-broker)
 _rd-kafka-metadata-partition
 (struct-out rd-kafka-metadata-partition)
 _rd-kafka-metadata-topic
 (struct-out rd-kafka-metadata-topic)
 _rd-kafka-metadata
 (struct-out rd-kafka-metadata))

;;; ---------------------------------
;;; @name Group interface
;;; ---------------------------------

(define-cstruct _rd-kafka-group-member-info
  ([member-id _string]
   [client-id _string]
   [client-host _string]
   [member-metadata _bytes]
   [member-metadata-size _size]
   [member-assignment _bytes]
   [member-assignment-size _size]))

(define-cstruct _rd-kafka-group-info
  ([broker _rd-kafka-metadata-broker]
   [group _string]
   [err _rd-kafka-resp-err]
   [state _string]
   [proto-type _string] ;; FIXME for some reason can't use protocol-type
   [protocol _string]
   [members _rd-kafka-group-member-info-pointer]
   [member-cnt _int]))

(define-cstruct _rd-kafka-group-list
  ([groups _rd-kafka-group-info-pointer]
   [group-cnt _int]))

(define-rdkafka rd-kafka-list-groups
  (_fun _rd-kafka-pointer _string
        (g : (_ptr o _pointer))
        _int
        -> (e : _rd-kafka-resp-err)
        -> (let* ([group-list (ptr-ref g _rd-kafka-group-list)]
                  [len (rd-kafka-group-list-group-cnt group-list)]
                  [groups (cblock->list (rd-kafka-group-list-groups group-list) _rd-kafka-group-info len)])
             (values e groups))))

(define-rdkafka rd-kafka-group-list-destroy
  (_fun _rd-kafka-group-list-pointer
        -> _void))

(provide
 _rd-kafka-group-member-info
 (struct-out rd-kafka-group-member-info)
 _rd-kafka-group-info
 (struct-out rd-kafka-group-info)
 _rd-kafka-group-list
 (struct-out rd-kafka-group-list)
 rd-kafka-list-groups
 rd-kafka-group-list-destroy)

(define x 0)
