package notifier

import (
	"crypto/md5"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/twitchscience/aws_utils/common"
)

var (
	Retrier = &common.Retrier{
		Times:         3,
		BackoffFactor: 2,
	}
)

type MessageCreator map[string]func(...interface{}) (string, error)

type SQSClient struct {
	Signer    *MessageCreator
	sqsClient sqsiface.SQSAPI
}

func BuildSQSClient(client sqsiface.SQSAPI) *SQSClient {
	m := make(MessageCreator)
	m.RegisterMessageType("error", func(args ...interface{}) (string, error) {
		return fmt.Sprintf("%v", args...), nil
	})
	return &SQSClient{
		Signer:    &m,
		sqsClient: client,
	}
}

func (s *SQSClient) SendMessage(messageType, qName string, args ...interface{}) error {
	out, err := s.sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(qName),
	})
	if err != nil {
		return fmt.Errorf("Error getting URL for SQS queue %s: %v", qName, err)
	}

	message, err := s.Signer.SignBody(messageType, args...)
	if err != nil {
		return err
	}
	return s.handle(message, aws.StringValue(out.QueueUrl))
}

func (s *SQSClient) handle(message, qURL string) error {
	var res *sqs.SendMessageOutput

	err := Retrier.Retry(func() error {
		var e error
		res, e = s.sqsClient.SendMessage(&sqs.SendMessageInput{
			QueueUrl:    aws.String(qURL),
			MessageBody: aws.String(message),
		})
		return e
	})

	if err != nil {
		return err
	}

	// check md5
	expectedHash := md5.New()
	expectedHash.Write([]byte(message))
	expected := fmt.Sprintf("%x", expectedHash.Sum(nil))
	if expected != aws.StringValue(res.MD5OfMessageBody) {
		return fmt.Errorf("message %s did not match expected %s", res.MD5OfMessageBody, expected)
	}
	return nil
}

type SNSClient struct {
	Signer    *MessageCreator
	snsClient snsiface.SNSAPI
}

func BuildSNSClient(client snsiface.SNSAPI) *SNSClient {
	m := make(MessageCreator)
	m.RegisterMessageType("error", func(args ...interface{}) (string, error) {
		return fmt.Sprintf("%v", args...), nil
	})
	return &SNSClient{
		Signer:    &m,
		snsClient: client,
	}
}

func (s *SNSClient) SendMessage(messageType, topicARN string, args ...interface{}) error {
	message, err := s.Signer.SignBody(messageType, args...)
	if err != nil {
		return err
	}

	err = Retrier.Retry(func() error {
		_, err := s.snsClient.Publish(&sns.PublishInput{
			Message:  &message,
			TopicArn: &topicARN,
		})
		return err
	})
	return err
}

func (m *MessageCreator) RegisterMessageType(name string, messageType func(...interface{}) (string, error)) {
	(*m)[name] = messageType
}

func (m *MessageCreator) SignBody(messageType string, args ...interface{}) (string, error) {
	if fn, ok := (*m)[messageType]; ok {
		return fn(args...)
	} else {
		return "", errors.New("message" + messageType + "does not exists")
	}
}
