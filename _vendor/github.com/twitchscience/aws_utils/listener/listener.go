package listener

// Listens to a sqs queue for messages.
// Similar to Http server listener uses a handler that forks on accept.
// Once handler is complete listener should delete the message.

// TODO make seperate sqs wrapper for queues
import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"fmt"
	"time"
)

type SQSHandler interface {
	Handle(*sqs.Message) error
}

type SQSListener struct {
	Handler SQSHandler

	sqsClient      sqsiface.SQSAPI
	pollInterval   time.Duration
	closeRequested chan bool
	closed         chan bool
}

func BuildSQSListener(handler SQSHandler, pollInterval time.Duration, client sqsiface.SQSAPI) *SQSListener {
	return &SQSListener{
		sqsClient:    client,
		pollInterval: pollInterval,
		Handler:      handler,
	}
}

func (l *SQSListener) Close() {
	close(l.closeRequested)
	<-l.closed
}

func (l *SQSListener) handle(msg *sqs.Message, qURL *string) {
	err := l.Handler.Handle(msg)
	if err != nil {
		fmt.Printf("SQS Handler returned error: %s\n", err)

		_, err = l.sqsClient.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
			QueueUrl:          qURL,
			ReceiptHandle:     msg.ReceiptHandle,
			VisibilityTimeout: aws.Int64(10), // seconds
		})

		if err != nil {
			fmt.Printf("Error setting message visibility: %s\n", err)
		}
		return
	}

	_, err = l.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      qURL,
		ReceiptHandle: msg.ReceiptHandle,
	})

	if err != nil {
		fmt.Println("unable to delete msg: ", err)
	}
}

func (l *SQSListener) waitForMessages(qURL *string) {
	o, err := l.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(1),
		QueueUrl:            qURL,
		VisibilityTimeout:   aws.Int64(10),
	})
	if err != nil || len(o.Messages) < 1 {
		time.Sleep(l.pollInterval)
		return
	}
	l.handle(o.Messages[0], qURL)
}

func (l *SQSListener) process(qURL *string) {
	for {
		select {
		case <-l.closeRequested:
			return
		default:
			l.waitForMessages(qURL)
		}
	}
}

func (l *SQSListener) Listen(qName string) {
	l.closeRequested = make(chan bool)
	l.closed = make(chan bool)

	defer close(l.closed)
	o, err := l.sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(qName),
	})
	if err != nil {
		fmt.Printf("Error getting URL for SQS queue %s: %v", qName, err)
		return
	}

	l.process(o.QueueUrl)
}
