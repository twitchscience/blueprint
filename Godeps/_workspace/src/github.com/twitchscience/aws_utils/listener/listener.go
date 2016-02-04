package listener

// Listens to a sqs queue for messages.
// Similar to Http server listener uses a handler that forks on accept.
// Once handler is complete listener should delete the message.

// TODO make seperate sqs wrapper for queues
import (
	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/sqs"

	"fmt"
	"time"
)

type SQSAddr struct {
	Region    aws.Region
	QueueName string
	Auth      aws.Auth
}

type SQSHandler interface {
	Handle(*sqs.Message) error
}

type SQSListener struct {
	Handler SQSHandler

	SQSClient      *sqs.SQS
	PollInterval   time.Duration
	Queue          *SQSAddr
	closeRequested chan bool
	closed         chan bool
}

func (a *SQSAddr) Network() string {
	return a.Region.Name
}

func (a *SQSAddr) String() string {
	return fmt.Sprintf("%s:%s", a.Region.Name, a.QueueName)
}

func makeSQSConnection(a *SQSAddr) *sqs.SQS {
	return sqs.New(a.Auth, a.Region)
}

func BuildSQSListener(addr *SQSAddr, handler SQSHandler, pollInterval time.Duration) *SQSListener {
	return &SQSListener{
		SQSClient:      sqs.New(addr.Auth, addr.Region),
		PollInterval:   pollInterval,
		Queue:          addr,
		Handler:        handler,
		closeRequested: make(chan bool),
		closed:         make(chan bool),
	}
}

func (l *SQSListener) Close() {
	l.closeRequested <- true
	<-l.closed
}

func (l *SQSListener) handle(msg *sqs.Message, q *sqs.Queue) {
	err := l.Handler.Handle(msg)
	if err != nil {
		q.ChangeMessageVisibility(msg, 10)
		fmt.Println(err)
		return
	}
	_, err = q.DeleteMessage(msg)
	if err != nil {
		fmt.Println("unable to delete msg: ", err)
	}
}

func (l *SQSListener) waitForMessages(q *sqs.Queue) {
	msgResponse, err := q.ReceiveMessage(1)
	if err != nil || len(msgResponse.Messages) < 1 {
		time.Sleep(l.PollInterval)
		return
	}
	l.handle(&msgResponse.Messages[0], q)
}

func (l *SQSListener) process(q *sqs.Queue) {
	for {
		select {
		case <-l.closeRequested:
			return
		default:
			l.waitForMessages(q)
		}
	}
}

func (l *SQSListener) Listen() {
	q, err := l.SQSClient.GetQueue(l.Queue.QueueName)
	if err != nil {
		fmt.Println("Specified queue ", l.Queue, " does not exist")
		return
	}

	l.process(q)
	l.closed <- true
}
