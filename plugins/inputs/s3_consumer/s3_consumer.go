// TODO add logic to remove sqs message upon 


package s3_consumer

import (
    "encoding/json"
    "strings"
    "bufio"

    "github.com/influxdata/telegraf"
    "github.com/influxdata/telegraf/plugins/inputs"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/service/sqs"
)

// S3Consumer is the top level struct for this plugin
type S3Consumer struct {
    // AWS
    Region string `toml:"region"`
    CredentialsPath string `toml:"credentials_path"`
    Role string `toml:"role"`
    AccessKey string `toml:"access_key"`
    SecretKey string `toml:"secret_key"`

    // S3
    Bucket string `toml:"bucket"`
    Prefix string `toml:"prefix"` // images/
    Suffix string `toml:"suffix"` // .jpg
    CompressionType bool `toml:"compression_type"` // none, gzip, bzip2
    EventTypes []string `toml:"event_types"` // i.e. 's3:ObjectCreated:*'. As listed in https://docs.aws.amazon.com/sdk-for-go/api/service/s3/

    // TODO: Do I want to support sending to an SNS topic + SQS queue or just direct to SQS queue?
    // SNS
    SNSTopic string `toml:"topic"`
    // Protocol string `toml:"protocol"`
    SNSSubscriptionFilter string `toml:"subscription_filter"`
    SNSTags []string `toml:"sns_tags"`

    // SQS
    PrefetchCount int `toml:"prefetch_count"` // number of sqs messages to read and put in channel
    SQSFetchCount int `toml:"sqs_fetch_count"` // number of sqs messages to pull at a time 1 to 10
    SQSName string `toml:"sqs_name"`
    // SQSType string `toml:"sqs_type"` // standard SQS queue is only allowed as an Amazon S3 event notification destination
    SQSVisibilityTimeout string `toml:"sqs_visibility_timeout"`
    SQSMessageRetentionPeriod string `toml:"sqs_message_retention_period"`
    SQSDeliveryDelay string `toml:"sqs_delivery_delay"`
    SQSMaximumMessageSize string `toml:"sqs_maximum_message_size"`
    SQSReceiveMessageWaitTime string `toml:"sqs_receive_message_wait_time"`
    SQSQueueOwners string `toml:"sqs_queue_owners"` // optional. otherwise set to "Only the queue owner"
    SQSEncryption bool `toml:"sqs_encryption"`
    SQSDeadLetterQueue bool `toml:"sqs_dead_letter_exchange"`
    SQSTags []string `toml:"sqs_tags"`

    // File
    parser parsers.Parser // TODO needed?
    decoder *encoding.Decoder // TODO needed?

    // AWS
    sess *Session
    sqs *SQS
    s3 *S3

    wg *sync.WaitGroup
    cancel context.CancelFunc
    Log telegraf.Logger

}

func (s *S3Consumer) Description() string {
    return "Consume S3Events and trigger reading metrics from file on creation event."
}

func (s *S3Consumer) SampleConfig() string {
    return `
  ## TODO
  ok = true
`
}

// init is for setup and validating config
/* perform init in start
func (s *S3Cosumer) Init() error {
    var err error
    s.decoder, err = encoding.NewDecoder(s.CharachterEncoding)/I
    return nil
}
*/

func (s *S3Consumer) assumeRole() (*session.Session, error) {
    sess := session.Must(session.NewSessionWithOptions(session.Options{
        SharedConfigState: session.SharedConfigEnable,
    }))

    // user, _ := user.Current() // Find a way to give the session a useful unique name maybe allow user to pass in something

    role, err := sts.New(sess).AssumeRole(&sts.AssumeRoleInput{
        RoleArn: aws.String(s.Role),
        RoleSessionName: aws.String("foo"), // TODO replace
    })
    if err != nil {
        return nil, fmt.Errorf("%w", err)
    }
    sess = session.New(aws.NewConfig().
        WithRegion(*sess.Config.Region).
        WithCredentials(credentials.NewStaticCredentials(
            *role.Credentials.AccessKeyId,
            *role.Credentials.SecretAccessKey,
            *role.Crednetials.SessionToken,
        )),
    )
    return sess, nil
}

func (s *S3Consumer) authenticateCredentials() (*session.Session, error) {
    if s.CredentialsFile != "" {
        sess, err := session.NewSession(&aws.Config{
            Region: aws.String(s.Region),
            Credentials: credentials.NewSharedCredentials(s.CredentialsPath, s.CredentialsProfile),
        }
    } else if s.AccessKey != "" and s.SecretKey != "" {
        sess, err := session.NewSession(&aws.Config{
            Region: aws.String(s.Region),
            Credentials: credentials.NewStaticCredentials(s.AccessKey, s.SecretKey, ""), // TODO what is the last arg
        })
    } else {
        // env or default cred files location
        sess, err := session.NewSession(&aws.Config{
            Region: aws.String(s.Region)},
        }
    }

    if err != nil {
        return nil, err
    }

    return sess, nil
}

// Gather
// Do we gather using the in-built pull style gather operation where we pull N sqs messages and read those files 
// or do we follow a more constant process where in the start we start a thread read sqs messages and pushing those
// events to N event processors reading files out of s3.
func (s *S3Consumer) Gather(_ telegraf.Accumulator) error {
    return nil
}

// poll sqs messages and write to buffered go channel
func (s *S3Consumer) sqsConsumer(msgs chan<- string) {
    defer s.wg.Done()
    var (
        event events.S3Event
    )

    for {
        sqsMsgs, err := s.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
            QueueUrl: s.SQSQueueUrl.QueueUrl,
            MaxNumberOfMessages: aws.Int64(1),
            WaitTimeSeconds: aws.Int64(s.SQSWaitTimeSeconds),
        })

        if err != nil {
            s.Log.Warnf("Failed to poll SQS: %v", err)
        }

        for _, message := range sqsMsgs.Messages {
            if err := json.Unmarshal(message.Body, event); err != nil {
                s.Log.Errorf("could not unmarshal event: %v", err)
                continue
            }
            for _, record := range event.Records {
                // Is there any reason to support multiple s3 event types?
                if strings.HasPrefix("ObjectCreated:") {
                    msgs <- record.S3 // S3Entity https://github.com/aws/aws-lambda-go/blob/master/events/s3.go https://github.com/aws/aws-lambda-go/blob/master/events/testdata/s3-event.json
                }
            }
        }
    }
    return nil
}

// read sqs message from go channel and read associated S3 file and write metrics to accumulator
func (s *S3Consumer) processor(msgs <-chan events.S3Entity, acc telegraf.Accumulator) error {
    defer s.wg.Done()
    for {
        entity := <-msgs

        req, err := s.s3.GetObject(&s3.GetObjectInput{
            Bucket: aws.String(entity.Bucket.Name),
            Key: aws.String(entity.S3Object.Key),
        })
        if err != nil {
            s.Log.Errorf("Failed to get S3 object %s/%s. %v", entity.Bucket.Name, entity.S3Object.Key, err)
            continue
        }

        body, err := ioutils.ReadAll(req.Body)
        if err != nil {
            s.Log.Errorf("Failed to read S3 object %s/%s. %v", entity.Bucket.Name, entity.S3Object.Key, err)
            continue
        }

        decoded, err := s.decoder.Decode(body)
        if err != nil {
            s.Log.Errorf("Failed to read S3 object %s/%s. %v", entity.Bucket.Name, entity.S3Object.Key, err)
            continue
        }

        metrics, err := s.parser.Parse(decoded)
        if err != nil {
            s.Log.Errorf("Failed to read S3 object %s/%s. %v", entity.Bucket.Name, entity.S3Object.Key, err)
            continue
        }

        for _, m := range metrics {
            acc.AddMetric(m)
        }
        /*
        // or should we try to read files in parts to allow for the occurance of large files without blowing out memory?
        scanner := bufio.NewScanner(req.Body)
        for scanner.Scan() {
            // fmt.Println(scanner.Text())
            body, err := a.decoder.Decode(scanner.Text())
            if err != nil {
                a.Log.Errorf
        }
        */
        req.Body.Close()
        // TODO delete sqs message now that the file has been read
    }
    return nil
}

func (s *S3Consumer) Start(acc telegraf.Accumulator) error {
    s.decoder, err = internal.NewContentDecorder(s.ContentEncoding)
    if err != nil {
        return err
    }

    if s.Role != "" {
        s.sess = s.assumeRole()
    } else {
        s.sess = s.authenticateCredentials()
    }

    s.sqs := sqs.New(s.sess)
    if err != nil {
        return err
    }

    s.s3 := s3.New(s.sess)
    if err != nil {
        return err
    }

    ctx, cancel := context.WithCancel(context.Background())
    s.cancel = cancel

    msgs := make(chan string, s.PrefetchCount)
    s.wg = &sync.WaitGroup{}
    s.wg.Add(1)
    go sqsConsumer(ctx, msgs)
    for w := 1; w <= s.Workers; w++ {
        wg.Add(1)
        go processor(ctx, msgs, acc)
    }

    return nil
}

func (s *S3Consumer) Stop() {
    s.cancel()
    s.wg.Wait()
}

func init() {
    inputs.Add("s3_consumer", func() telegraf.Input { return &S3Consumer{} })
}
