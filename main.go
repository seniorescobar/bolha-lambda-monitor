package main

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/seniorescobar/bolha/client"

	log "github.com/sirupsen/logrus"
)

const (
	tableName      = "Bolha"
	s3ImagesBucket = "bolha-images"
)

var (
	ddbc *dynamodb.DynamoDB
	s3d  *s3manager.Downloader
)

type BolhaItem struct {
	AdTitle       string
	AdDescription string
	AdPrice       int
	AdCategoryId  int
	AdImages      []string

	AdUploadedId int64
	AdUploadedAt string

	UserUsername  string
	UserPassword  string
	UserSessionId string

	ReuploadHours int
	ReuploadOrder int
}

func Handler(ctx context.Context) error {
	sess := session.Must(session.NewSession())

	// initialize aws service clients
	ddbc = dynamodb.New(sess)
	s3d = s3manager.NewDownloader(sess)

	// get all items
	bItems, err := getBolhaItems()
	if err != nil {
		return err
	}

	for _, bItem := range bItems {
		// create new client
		var (
			c   *client.Client
			err error
		)
		if bItem.UserSessionId != "" {
			c, err = client.NewWithSessionId(bItem.UserSessionId)
		} else {
			c, err = client.New(&client.User{bItem.UserUsername, bItem.UserPassword})
		}
		if err != nil {
			return err
		}

		// get active (uploaded) ad
		activeAd, err := c.GetActiveAd(bItem.AdUploadedId)
		if err != nil {
			return err
		}

		// parse ad uploaded at
		adUploadedAt, err := time.Parse(time.RFC3339, bItem.AdUploadedAt)
		if err != nil {
			return err
		}

		// check re-upload condition
		if checkOrder(activeAd.Order, bItem.ReuploadOrder) || checkTimeDiff(adUploadedAt, bItem.ReuploadHours) {
			// reupload ad = remove + upload

			// remove ad
			if err := c.RemoveAd(bItem.AdUploadedId); err != nil {
				return err
			}

			// download s3 images
			images := make([]io.Reader, len(bItem.AdImages))
			for i, imgPath := range bItem.AdImages {
				img, err := downloadS3Image(imgPath)
				if err != nil {
					return err
				}

				images[i] = img
			}

			// upload ad
			newUploadedId, err := c.UploadAd(&client.Ad{
				Title:       bItem.AdTitle,
				Description: bItem.AdDescription,
				Price:       bItem.AdPrice,
				CategoryId:  bItem.AdCategoryId,
				Images:      images,
			})
			if err != nil {
				return err
			}

			// update uploaded id
			if err := updateUploadedId(bItem.AdTitle, newUploadedId); err != nil {
				return err
			}
		}
	}

	return nil
}

func checkOrder(currOrder, allowedOrder int) bool {
	return currOrder > allowedOrder
}

func checkTimeDiff(currUploadedAt time.Time, allowedHours int) bool {
	return time.Since(currUploadedAt) > time.Duration(allowedHours)*time.Hour
}

// DYNAMODB

func getBolhaItems() ([]BolhaItem, error) {
	filt := expression.Name("AdUploadedId").GreaterThan(expression.Value(0))
	expr, err := expression.NewBuilder().WithFilter(filt).Build()
	if err != nil {
		return nil, err
	}

	result, err := ddbc.Scan(&dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		TableName:                 aws.String(tableName),
	})
	if err != nil {
		return nil, err
	}

	bItems := make([]BolhaItem, 0)
	if err := dynamodbattribute.UnmarshalListOfMaps(result.Items, &bItems); err != nil {
		return nil, err
	}

	return bItems, nil
}

func updateUploadedId(adTitle string, adUploadedId int64) error {
	_, err := ddbc.UpdateItem(&dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":uploadedId": {N: aws.String(strconv.FormatInt(adUploadedId, 10))},
			":uploadedAt": {S: aws.String(time.Now().Format(time.RFC3339))},
		},
		Key:              map[string]*dynamodb.AttributeValue{"AdTitle": {S: aws.String(adTitle)}},
		UpdateExpression: aws.String("SET AdUploadedId = :uploadedId, AdUploadedAt = :uploadedAt"),
		TableName:        aws.String(tableName),
	})

	return err
}

// S3

func downloadS3Image(imgKey string) (io.Reader, error) {
	log.WithField("imgKey", imgKey).Info("downloading image from s3")

	buff := new(aws.WriteAtBuffer)

	_, err := s3d.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(s3ImagesBucket),
		Key:    aws.String(imgKey),
	})
	if err != nil {
		return nil, err
	}

	imgBytes := buff.Bytes()

	return bytes.NewReader(imgBytes), nil
}

func main() {
	lambda.Start(Handler)
}
