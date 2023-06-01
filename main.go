package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	// Importing the types package of your blog blockchain
	data "Dhpc/x/data/types"

	request "Dhpc/x/request/types"

	"github.com/ignite/cli/ignite/pkg/cosmosaccount"
	"github.com/ignite/cli/ignite/pkg/cosmosclient"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

type Miner struct {
	mu        sync.RWMutex
	responses map[string]*request.MinerResponse
	account   cosmosaccount.Account
	address   string
	client    cosmosclient.Client
	ctx       context.Context
}

var rootCmd = &cobra.Command{
	Use:   "miner",
	Short: "Miner is a CLI client for DHPC network.",
	Run: func(cmd *cobra.Command, args []string) {
		// get environment variables
		accountName, _ := cmd.Flags().GetString("account")
		if accountName == "" {
			log.Fatal("Please provide an account using the --account flag")
		}
		apiKey := os.Getenv("API_KEY")

		// // validate the environment variables
		if apiKey == "" {
			log.Fatal("Please set the API_KEY, environment variables")
		}

		ctx := context.Background()

		// Create a Cosmos client instance
		// TODO: doesn't reconnect if the connection is lost
		client, err := cosmosclient.New(ctx, cosmosclient.WithAddressPrefix("dhpc"))
		if err != nil {
			log.Fatal(err)
		}

		// Get account from the keyring
		account, err := client.Account(accountName)
		if err != nil {
			log.Fatal(err)
		}

		// construct the miner
		miner := NewMiner(account, client, ctx)

		// start the miner
		miner.Start()
	},
}

func main() {
	rootCmd.PersistentFlags().String("account", "", "Account to use for mining")
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

// Initialize new miner
func NewMiner(account cosmosaccount.Account, client cosmosclient.Client, ctx context.Context) *Miner {
	addr, err := account.Address("dhpc")
	if err != nil {
		log.Fatal(err)
	}

	log.WithField("address", addr).Info("Starting miner")
	return &Miner{
		responses: make(map[string]*request.MinerResponse),
		account:   account,
		client:    client,
		address:   addr,
		ctx:       ctx,
	}
}

// Example of how you can start miner. This function is not complete.
func (m *Miner) Start() {

	queryClient := request.NewQueryClient(m.client.Context())

	for {
		queryResp, err := queryClient.RequestRecordAllMinerPending(m.ctx, &request.QueryAllRequestRecordRequest{})
		if err != nil {
			log.Print(err)
		}
		for _, record := range queryResp.RequestRecord {
			log.WithFields(logrus.Fields{"UUID": record.UUID, "Network": record.Network, "Address": record.Address}).Info("Received a record from miner_pending")
			m.processMinerPendingRecord(record)
		}

		time.Sleep(2 * time.Second)
	}
}

// Here start two goroutines: one to monitor miner_pending, another to monitor answer_pending

// Processes a record from miner_pending
func (m *Miner) processMinerPendingRecord(record request.RequestRecord) {
	m.mu.RLock()
	_, exist := m.responses[record.UUID]
	m.mu.RUnlock()

	if exist {
		log.WithFields(logrus.Fields{"UUID": record.UUID}).Info("Record already processed")
		return
	}

	m.mu.RLock()
	m.responses[record.UUID] = &request.MinerResponse{}
	m.mu.RUnlock()

	log.WithFields(logrus.Fields{"UUID": record.UUID}).Info("Processing record")

	// Generate dataUsed
	dataQueryClient := data.NewQueryClient(m.client.Context())

	queryResp, err := dataQueryClient.DataAllByAddr(m.ctx, &data.QueryAllDataRequestByAddr{Address: record.Address})
	if err != nil {
		log.WithFields(logrus.Fields{"UUID": record.UUID}).Error("Error in getting data from data module")
	}

	var dataUsedArray []string
	var answer int32
	for _, data := range queryResp.Data {
		dataUsedArray = append(dataUsedArray, data.Hash)
		answer = answer + data.Score
	}
	if len(dataUsedArray) > 0 {
		// TODO: This is weak, should be based on weighted average of scores based on user reputation
		answer = answer / int32(len(dataUsedArray))
	} else {
		// TODO: if this is zero, it doesn't get carried on the message and causes havoc
		answer = int32(0)
	}

	dataUsed := strings.Join(dataUsedArray, ",")

	// Generate a random salt, make sure it's always positive
	salt := int32(rand.Intn(100000))

	// Generate sumStr
	spew.Dump(answer, salt)
	sumStr := strconv.Itoa(int(answer) + int(salt))

	// Generate a random UUID
	minerUUID := uuid.New().String()

	// Generate MD5 hash for answer and salt
	hash := md5.New()
	hash.Write([]byte(sumStr))
	md5sum := hex.EncodeToString(hash.Sum(nil))

	// Create a MinerResponse
	response := &request.MinerResponse{
		UUID:        minerUUID,
		RequestUUID: record.UUID,
		Hash:        md5sum,
		DataUsed:    dataUsed,
		Answer:      answer,
		Salt:        salt,
		Creator:     m.address,
	}

	msg := request.NewMsgCreateMinerResponse(
		m.address,
		minerUUID,
		record.UUID,
		md5sum,
		dataUsed,
	)

	txResp, err := m.client.BroadcastTx(m.ctx, m.account, msg)
	if err != nil {
		log.WithFields(logrus.Fields{"UUID": record.UUID, "LOG": txResp.RawLog, "TXHash": txResp.TxHash, "Error": err}).Error("Error when broadcasting tx")
		log.Print(err)
	}

	log.WithFields(logrus.Fields{"UUID": record.UUID, "LOG": txResp.RawLog, "TXHash": txResp.TxHash}).Info("Processing record")

	m.mu.Lock()
	m.responses[record.UUID] = response
	m.mu.Unlock()

	m.processAnswerPendingRecord(*response)

}

// Processes a record from answer_pending
func (m *Miner) processAnswerPendingRecord(record request.MinerResponse) {
	queryClient := request.NewQueryClient(m.client.Context())
	requestRecord := request.QueryGetRequestRecordRequest{
		UUID: record.RequestUUID,
	}
	for {
		queryResp, err := queryClient.RequestRecord(m.ctx, &requestRecord)
		if err != nil {
			log.WithFields(logrus.Fields{"UUID": record.RequestUUID}).Error("Error when querying request record at stage 1")
		}

		if queryResp.RequestRecord.Stage == 1 {
			log.WithFields(logrus.Fields{"UUID": record.RequestUUID}).Info("Request is in stage 1")
			msg := request.NewMsgUpdateMinerResponse(
				m.address,
				record.UUID,
				record.RequestUUID,
				record.Answer,
				record.Salt,
			)
			spew.Dump(record)
			txResp, err := m.client.BroadcastTx(m.ctx, m.account, msg)
			if err != nil {
				log.WithFields(logrus.Fields{"UUID": record.UUID, "LOG": txResp.RawLog, "TXHash": txResp.TxHash, "Error": err}).Error("Error when broadcasting tx at stage 1")
			}
			log.WithFields(logrus.Fields{"UUID": record.UUID, "Answer": record.Answer, "TXHash": txResp.TxHash}).Info("Broadcasted tx at stage 1")
			// TODO: clean m.responses[record.UUID], remove record that we just used
			m.responses[record.UUID] = nil
			break
		}
	}
}
