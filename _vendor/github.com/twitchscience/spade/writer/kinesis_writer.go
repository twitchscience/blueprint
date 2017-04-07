package writer

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twinj/uuid"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/spade/batcher"
	"github.com/twitchscience/spade/globber"
)

const (
	// RedshiftDatetimeIngestString is the format of timestamps that Redshift understands.
	RedshiftDatetimeIngestString = "2006-01-02 15:04:05.999"
)

// KinesisWriterConfig is used to configure a KinesisWriter
// and its nested globber and batcher objects
type KinesisWriterConfig struct {
	StreamName             string
	StreamRole             string
	StreamType             string // StreamType should be either "stream" or "firehose"
	Compress               bool   // true if compress data with flate, false to output json
	FirehoseRedshiftStream bool   // true if JSON destined for Firehose->Redshift streaming
	BufferSize             int
	MaxAttemptsPerRecord   int
	RetryDelay             string

	Events map[string]*struct {
		Filter     string
		FilterFunc func(map[string]string) bool `json:"-"`
		Fields     []string
	}

	Globber globber.Config
	Batcher batcher.Config
}

// Validate returns an error if the config is not valid, or nil if it is.
// It also sets the FilterFunc on Events with Filters.
func (c *KinesisWriterConfig) Validate() error {
	err := c.Globber.Validate()
	if err != nil {
		return fmt.Errorf("globber config invalid: %s", err)
	}

	err = c.Batcher.Validate()
	if err != nil {
		return fmt.Errorf("batcher config invalid: %s", err)
	}

	for _, e := range c.Events {
		if e.Filter != "" {
			e.FilterFunc = filterFuncs[e.Filter]
			if e.FilterFunc == nil {
				return fmt.Errorf("batcher config invalid: %s", err)
			}
		}
	}

	if c.FirehoseRedshiftStream && (c.StreamType != "firehose" || c.Compress) {
		return fmt.Errorf("Redshift streaming only valid with non-compressed firehose")
	}

	_, err = time.ParseDuration(c.RetryDelay)
	return err
}

var filterFuncs = map[string]func(map[string]string) bool{
	"isVod": func(fields map[string]string) bool {
		return fields["vod_id"] != "" && fields["vod_type"] != "clip"
	},
}

// Statter sends stats for a BatchWriter.
type Statter struct {
	statter   statsd.Statter
	statNames map[int]string
}

// NewStatter returns a Statter for the given stream.
func NewStatter(statter statsd.Statter, streamName string) *Statter {
	return &Statter{
		statter:   statter,
		statNames: generateStatNames(streamName),
	}
}

// IncStat increments a stat by an amount on the Statter.
func (w *Statter) IncStat(stat int, amount int64) {
	if amount != 0 {
		err := w.statter.Inc(w.statNames[stat], amount, 1)
		if err != nil {
			logger.WithError(err).WithField("statName", w.statNames[stat]).
				Error("Failed to put stat")
		}
	}
}

// BatchWriter is an interface to write batches to an external sink.
type BatchWriter interface {
	SendBatch([][]byte)
}

// StreamBatchWriter writes batches to Kinesis Streams
type StreamBatchWriter struct {
	client  kinesisiface.KinesisAPI
	config  *KinesisWriterConfig
	statter *Statter
}

// FirehoseBatchWriter writes batches to Kinesis Firehose
type FirehoseBatchWriter struct {
	client  firehoseiface.FirehoseAPI
	config  *KinesisWriterConfig
	statter *Statter
}

// KinesisWriter is a writer that writes events to kinesis
type KinesisWriter struct {
	incoming    chan *WriteRequest
	batches     chan [][]byte
	globber     *globber.Globber
	batcher     *batcher.Batcher
	config      KinesisWriterConfig
	batchWriter BatchWriter

	sync.WaitGroup
}

const (
	statPutRecordsAttempted = iota
	statPutRecordsLength
	statPutRecordsErrors
	statRecordsFailedThrottled
	statRecordsFailedInternalError
	statRecordsFailedUnknown
	statRecordsSucceeded
	statRecordsDropped
)

func generateStatNames(streamName string) map[int]string {
	stats := make(map[int]string)
	stats[statPutRecordsAttempted] = "kinesiswriter." + streamName + ".putrecords.attempted"
	stats[statPutRecordsLength] = "kinesiswriter." + streamName + ".putrecords.length"
	stats[statPutRecordsErrors] = "kinesiswriter." + streamName + ".putrecords.errors"
	stats[statRecordsFailedThrottled] = "kinesiswriter." + streamName + ".records_failed.throttled"
	stats[statRecordsFailedInternalError] = "kinesiswriter." + streamName + ".records_failed.internal_error"
	stats[statRecordsFailedUnknown] = "kinesiswriter." + streamName + ".records_failed.unknown_reason"
	stats[statRecordsSucceeded] = "kinesiswriter." + streamName + ".records_succeeded"
	stats[statRecordsDropped] = "kinesiswriter." + streamName + ".records_dropped"

	return stats
}

// NewKinesisWriter returns an instance of SpadeWriter that writes
// events to kinesis
func NewKinesisWriter(session *session.Session, statter statsd.Statter, config KinesisWriterConfig) (SpadeWriter, error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	var batchWriter BatchWriter
	if config.StreamRole != "" {
		credentials := stscreds.NewCredentials(
			session,
			config.StreamRole,
			func(provider *stscreds.AssumeRoleProvider) {
				provider.ExpiryWindow = time.Minute
			})
		session = session.Copy(&aws.Config{Credentials: credentials})
	}
	wStatter := NewStatter(statter, config.StreamName)
	switch config.StreamType {
	case "stream":
		batchWriter = &StreamBatchWriter{kinesis.New(session), &config, wStatter}
	case "firehose":
		batchWriter = &FirehoseBatchWriter{firehose.New(session), &config, wStatter}
	default:
		return nil, fmt.Errorf("unknown stream type: %s", config.StreamType)
	}
	w := &KinesisWriter{
		incoming:    make(chan *WriteRequest, config.BufferSize),
		batches:     make(chan [][]byte),
		config:      config,
		batchWriter: batchWriter,
	}

	w.batcher, err = batcher.New(config.Batcher, func(b [][]byte) {
		w.batches <- b
	})
	if err != nil {
		return nil, err
	}

	w.globber, err = globber.New(config.Globber, func(b []byte) {
		w.batcher.Submit(b)
	})
	if err != nil {
		return nil, err
	}

	w.Add(2)
	logger.Go(w.incomingWorker)
	logger.Go(w.sendWorker)
	return w, nil
}

// Write is the entry point for events into the kinesis
// writer, assuming the event is not filtered it will
// eventually be written to Kinesis as part of a flate
// compressed json blob

func (w *KinesisWriter) Write(req *WriteRequest) {
	w.incoming <- req
}

func (w *KinesisWriter) submit(name string, columns map[string]string) {
	event, ok := w.config.Events[name]
	if !ok {
		return
	}
	if event.FilterFunc != nil && !event.FilterFunc(columns) {
		return
	}

	pruned := make(map[string]string)
	for _, field := range event.Fields {
		if val, ok := columns[field]; ok {
			pruned[field] = val
		} else {
			pruned[field] = ""
		}
	}

	if len(pruned) > 0 {
		// if we want data compressed, we send it to globber
		if w.config.Compress {
			err := w.globber.Submit(struct {
				Name   string
				Fields map[string]string
			}{
				Name:   name,
				Fields: pruned,
			})
			if err != nil {
				logger.WithError(err).WithField("name", name).Error(
					"Failed to Submit to globber")
			}
		} else {
			e, _ := json.Marshal(pruned)
			w.batcher.Submit(e)
		}
	}
}

func (w *KinesisWriter) incomingWorker() {
	defer w.Done()

	defer func() {
		// tell the globber to flush itself
		w.globber.Close()

		// tell the batcher to flush itself
		w.batcher.Close()

		// done with the batches channel
		close(w.batches)
	}()

	for {
		req, ok := <-w.incoming
		if !ok {
			return
		}
		w.submit(req.Category, req.Record)
	}
}

func (w *KinesisWriter) sendWorker() {
	defer w.Done()

	for {
		batch, ok := <-w.batches
		if !ok {
			return
		}
		w.Add(1)
		logger.Go(func() {
			defer w.Done()
			w.batchWriter.SendBatch(batch)
		})
	}
}

const version = 1

type record struct {
	UUID      string
	Version   int
	Data      []byte
	CreatedAt string
}

type jsonRecord struct {
	UUID      string
	Version   int
	Data      map[string]string
	CreatedAt string
}

// SendBatch writes the given batch to a stream, configured by the KinesisWriter
func (w *StreamBatchWriter) SendBatch(batch [][]byte) {
	if len(batch) == 0 {
		return
	}

	records := make([]*kinesis.PutRecordsRequestEntry, len(batch))
	for i, e := range batch {
		UUID := uuid.NewV4()
		var data []byte
		var marshalErr error
		var unmarshalErr error
		if w.config.Compress {
			// if we are sending compressed data, send it as is
			data, marshalErr = json.Marshal(&record{
				UUID:      UUID.String(),
				Version:   version,
				Data:      e,
				CreatedAt: time.Now().UTC().Format(RedshiftDatetimeIngestString),
			})
		} else {
			// if sending uncompressed json, remarshal batch into new objects
			var unpacked map[string]string
			unmarshalErr = json.Unmarshal(e, &unpacked)
			data, marshalErr = json.Marshal(&jsonRecord{
				UUID:      UUID.String(),
				Version:   version,
				Data:      unpacked,
				CreatedAt: time.Now().UTC().Format(RedshiftDatetimeIngestString),
			})
		}
		if marshalErr != nil {
			logger.WithField("stream", w.config.StreamName).
				WithError(marshalErr).
				Error("Failed to marshal into Record")
		}
		if unmarshalErr != nil {
			logger.WithField("stream", w.config.StreamName).
				WithError(unmarshalErr).
				Error("Failed to unmarshal into Record")
		}
		records[i] = &kinesis.PutRecordsRequestEntry{
			PartitionKey: aws.String(UUID.String()),
			Data:         data,
		}
	}

	retryDelay, _ := time.ParseDuration(w.config.RetryDelay)

	args := &kinesis.PutRecordsInput{
		StreamName: aws.String(w.config.StreamName),
		Records:    records,
	}

	var err error
	for attempt := 1; attempt <= w.config.MaxAttemptsPerRecord; attempt++ {
		w.statter.IncStat(statPutRecordsAttempted, 1)
		w.statter.IncStat(statPutRecordsLength, int64(len(records)))

		var res *kinesis.PutRecordsOutput
		res, err = w.client.PutRecords(args)

		if err != nil {
			logger.WithError(err).WithFields(map[string]interface{}{
				"attempt":      attempt,
				"max_attempts": w.config.MaxAttemptsPerRecord,
				"stream":       w.config.StreamName,
			}).Warn("Failed to put records")
			w.statter.IncStat(statPutRecordsErrors, 1)
			time.Sleep(retryDelay)
			continue
		}

		// Find all failed records and update the slice to contain only failures
		retryCount := 0
		var provisionThroughputExceeded, internalFailure, unknownError, succeeded int64
		for j, result := range res.Records {
			if aws.StringValue(result.ErrorCode) != "" {
				switch aws.StringValue(result.ErrorCode) {
				case "ProvisionedThroughputExceededException":
					provisionThroughputExceeded++
				case "InternalFailure":
					internalFailure++
				default:
					// Something undocumented
					unknownError++
				}
				args.Records[retryCount] = args.Records[j]
				retryCount++
			} else {
				succeeded++
			}
		}
		w.statter.IncStat(statRecordsFailedThrottled, provisionThroughputExceeded)
		w.statter.IncStat(statRecordsFailedInternalError, internalFailure)
		w.statter.IncStat(statRecordsFailedUnknown, unknownError)
		w.statter.IncStat(statRecordsSucceeded, succeeded)
		args.Records = args.Records[:retryCount]

		if retryCount == 0 {
			return
		}

		time.Sleep(retryDelay)
	}
	logger.WithField("failures", len(args.Records)).
		WithField("attempts", len(records)).
		WithField("stream", w.config.StreamName).
		WithError(err).
		Error("Failed to send records to Kinesis")
	w.statter.IncStat(statRecordsDropped, int64(len(args.Records)))
}

// SendBatch writes the given batch to a firehose, configured by the KinesisWriter
func (w *FirehoseBatchWriter) SendBatch(batch [][]byte) {
	if len(batch) == 0 {
		return
	}

	records := make([]*firehose.Record, len(batch))
	for i, e := range batch {
		UUID := uuid.NewV4()
		var data []byte
		var marshalErr error
		var unmarshalErr error
		if w.config.Compress {
			// if we are sending compressed data, send it as is
			data, marshalErr = json.Marshal(&record{
				UUID:    UUID.String(),
				Version: version,
				Data:    e,
			})
		} else if w.config.FirehoseRedshiftStream {
			// if streaming data to Redshift, send data as top level JSON and scrub away null bytes
			var unpacked map[string]string
			unmarshalErr = json.Unmarshal(e, &unpacked)
			for k, v := range unpacked {
				unpacked[k] = strings.Replace(v, "\x00", "", -1)
			}
			data, marshalErr = json.Marshal(unpacked)
		} else {
			// if sending uncompressed json, remarshal batch into new objects
			var unpacked map[string]string
			unmarshalErr = json.Unmarshal(e, &unpacked)
			data, marshalErr = json.Marshal(&jsonRecord{
				UUID:    UUID.String(),
				Version: version,
				Data:    unpacked,
			})
		}
		if marshalErr != nil {
			logger.WithField("firehose", w.config.StreamName).
				WithError(marshalErr).
				Error("Failed to marshal into Record")
		}
		if unmarshalErr != nil {
			logger.WithField("firehose", w.config.StreamName).
				WithError(unmarshalErr).
				Error("Failed to unmarshal into Record")
		}
		// Add '\n' as a record separator
		data = append(data, '\n')
		records[i] = &firehose.Record{
			Data: data,
		}
	}

	retryDelay, _ := time.ParseDuration(w.config.RetryDelay)

	args := &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(w.config.StreamName),
		Records:            records,
	}

	var err error
	for attempt := 1; attempt <= w.config.MaxAttemptsPerRecord; attempt++ {
		w.statter.IncStat(statPutRecordsAttempted, 1)
		w.statter.IncStat(statPutRecordsLength, int64(len(records)))

		var res *firehose.PutRecordBatchOutput
		res, err = w.client.PutRecordBatch(args)

		if err != nil {
			logger.WithError(err).WithFields(map[string]interface{}{
				"attempt":      attempt,
				"max_attempts": w.config.MaxAttemptsPerRecord,
				"stream":       w.config.StreamName,
			}).Warn("Failed to put record batch")
			w.statter.IncStat(statPutRecordsErrors, 1)
			time.Sleep(retryDelay)
			continue
		}

		// Find all failed records and update the slice to contain only failures
		retryCount := 0
		var provisionThroughputExceeded, internalFailure, unknownError, succeeded int64
		for j, result := range res.RequestResponses {
			if aws.StringValue(result.ErrorCode) != "" {
				switch aws.StringValue(result.ErrorCode) {
				case "ProvisionedThroughputExceededException":
					provisionThroughputExceeded++
				case "InternalFailure":
					internalFailure++
				default:
					// Something undocumented
					unknownError++
				}
				args.Records[retryCount] = args.Records[j]
				retryCount++
			} else {
				succeeded++
			}
		}
		w.statter.IncStat(statRecordsFailedThrottled, provisionThroughputExceeded)
		w.statter.IncStat(statRecordsFailedInternalError, internalFailure)
		w.statter.IncStat(statRecordsFailedUnknown, unknownError)
		w.statter.IncStat(statRecordsSucceeded, succeeded)
		args.Records = args.Records[:retryCount]

		if retryCount == 0 {
			return
		}

		time.Sleep(retryDelay)
	}
	logger.WithField("failures", len(args.Records)).
		WithField("attempts", len(records)).
		WithField("stream", w.config.StreamName).
		WithError(err).
		Error("Failed to send records to Firehose")
	w.statter.IncStat(statRecordsDropped, int64(len(args.Records)))
}

// Close closes a KinesisWriter
func (w *KinesisWriter) Close() error {
	close(w.incoming)
	w.Wait()
	return nil
}

// Rotate doesn't do anything as KinesisWriters don't need to
// rotate.
func (w *KinesisWriter) Rotate() (bool, error) {
	return true, nil
}
