package main

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
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

	var wg sync.WaitGroup

	errChan := make(chan error)

	for _, bi := range bItems {
		bItem := bi

		wg.Add(1)

		go func() {
			defer wg.Done()

			// create new client
			c, err := client.NewWithSessionId(bItem.UserSessionId)
			if err != nil {
				errChan <- err
				return
			}

			// upload if not yet uploaded
			if bItem.AdUploadedId == 0 {
				newUploadedId, err := uploadAd(c, bItem)
				if err != nil {
					errChan <- err
					return
				}

				// update uploaded id
				if err := updateUploadedId(bItem.AdTitle, newUploadedId); err != nil {
					errChan <- err
					return
				}

				return
			}

			// get active (uploaded) ad
			activeAd, err := c.GetActiveAd(bItem.AdUploadedId)
			if err != nil {
				errChan <- err
				return
			}

			adUploadedAtParsed, err := time.Parse(time.RFC3339, bItem.AdUploadedAt)
			if err != nil {
				errChan <- err
				return
			}

			// if ad not old
			if activeAd.Order > bItem.ReuploadOrder || time.Since(adUploadedAtParsed) > time.Duration(bItem.ReuploadHours)*time.Hour {
				removeChan := make(chan struct{}, 1)
				uploadChan := make(chan int64, 1)
				errChan1 := make(chan error, 2)

				// remove
				go func() {
					if err := c.RemoveAd(bItem.AdUploadedId); err != nil {
						errChan1 <- err
						return
					}
					removeChan <- struct{}{}
				}()

				// upload
				go func() {
					newUploadedId, err := uploadAd(c, bItem)
					if err != nil {
						errChan1 <- err
						return
					}
					uploadChan <- newUploadedId
				}()

				go func() {
					<-removeChan
					newUploadedId := <-uploadChan

					// update uploaded id
					if err := updateUploadedId(bItem.AdTitle, newUploadedId); err != nil {
						errChan1 <- err
						return
					}

					close(errChan1)
				}()

				for err := range errChan1 {
					errChan <- err
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		return err
	}

	return nil
}

// HELPERS
func uploadAd(c *client.Client, bItem BolhaItem) (int64, error) {
	// download s3 images
	s3Images, err := downloadS3Images(bItem.AdImages)
	if err != nil {
		return 0, err
	}

	// upload ad
	return c.UploadAd(&client.Ad{
		Title:       bItem.AdTitle,
		Description: bItem.AdDescription,
		Price:       bItem.AdPrice,
		CategoryId:  bItem.AdCategoryId,
		Images:      s3Images,
	})
}

func downloadS3Images(images []string) ([]io.Reader, error) {
	// do not use img chan because images need to maintain initial order
	var wg sync.WaitGroup

	errChan := make(chan error, len(images))

	s3Images := make([]io.Reader, len(images))
	for i, imgPath := range images {
		i1, imgPath1 := i, imgPath

		wg.Add(1)

		go func() {
			defer wg.Done()

			img, err := downloadS3Image(imgPath1)
			if err != nil {
				errChan <- err
				return
			}

			s3Images[i1] = img
		}()
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		return nil, err
	}

	return s3Images, nil
}

// DYNAMODB

func getBolhaItems() ([]BolhaItem, error) {
	result, err := ddbc.Scan(&dynamodb.ScanInput{
		TableName: aws.String(tableName),
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
