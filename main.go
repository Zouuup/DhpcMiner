package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	// Importing the types package of your blog blockchain
	"Dhpc/x/request/types"

	"github.com/ignite/cli/ignite/pkg/cosmosaccount"
	"github.com/ignite/cli/ignite/pkg/cosmosclient"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

type Miner struct {
	mu        sync.RWMutex
	responses map[string]*types.MinerResponse
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
		responses: make(map[string]*types.MinerResponse),
		account:   account,
		client:    client,
		address:   addr,
		ctx:       ctx,
	}
}

// Example of how you can start miner. This function is not complete.
func (m *Miner) Start() {

	queryClient := types.NewQueryClient(m.client.Context())

	for {
		queryResp, err := queryClient.RequestRecordAllMinerPending(m.ctx, &types.QueryAllRequestRecordRequest{})
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
func (m *Miner) processMinerPendingRecord(record types.RequestRecord) {
	m.mu.RLock()
	_, exist := m.responses[record.UUID]
	m.mu.RUnlock()

	if exist {
		log.WithFields(logrus.Fields{"UUID": record.UUID}).Info("Record already processed")
		return
	}

	m.mu.RLock()
	m.responses[record.UUID] = &types.MinerResponse{}
	m.mu.RUnlock()

	log.WithFields(logrus.Fields{"UUID": record.UUID}).Info("Processing record")

	// Generate random number between 1-1000 for answer and hashused
	answer := int32(rand.Intn(1000) + 1)

	// Generate salt
	salt := int32(rand.Intn(10000000000))

	// Generate sumStr
	sumStr := strconv.Itoa(int(answer) + int(salt))

	// Generate dataUsed
	dataUsed := "fe13119fb084fe8bbf5fe3ab7cc89b3b,3b5d5c3712955042212316173ccf37be,2cd6ee2c70b0bde53fbe6cac3c8b8bb1"

	// Generate a random UUID
	minerUUID := uuid.New().String()

	// Generate MD5 hash for answer and salt
	hash := md5.New()
	hash.Write([]byte(sumStr))
	md5sum := hex.EncodeToString(hash.Sum(nil))

	// Create a MinerResponse
	response := &types.MinerResponse{
		UUID:        minerUUID,
		RequestUUID: record.UUID,
		Hash:        md5sum,
		DataUsed:    dataUsed,
		Answer:      answer,
		Salt:        salt,
		Creator:     m.address,
	}

	msg := types.NewMsgCreateMinerResponse(
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
func (m *Miner) processAnswerPendingRecord(record types.MinerResponse) {
	queryClient := types.NewQueryClient(m.client.Context())
	request := types.QueryGetRequestRecordRequest{
		UUID: record.RequestUUID,
	}
	for {
		queryResp, err := queryClient.RequestRecord(m.ctx, &request)
		if err != nil {
			log.WithFields(logrus.Fields{"UUID": record.RequestUUID}).Error("Error when querying request record at stage 1")
		}

		if queryResp.RequestRecord.Stage == 1 {
			log.WithFields(logrus.Fields{"UUID": record.RequestUUID}).Info("Request is in stage 1")
			msg := types.NewMsgUpdateMinerResponse(
				m.address,
				record.UUID,
				record.RequestUUID,
				record.Answer,
				record.Salt,
			)
			txResp, err := m.client.BroadcastTx(m.ctx, m.account, msg)
			if err != nil {
				log.WithFields(logrus.Fields{"UUID": record.UUID, "LOG": txResp.RawLog, "TXHash": txResp.TxHash, "Error": err}).Error("Error when broadcasting tx at stage 1")
			}
			log.WithFields(logrus.Fields{"UUID": record.UUID, "LOG": txResp.RawLog, "TXHash": txResp.TxHash}).Info("Broadcasted tx at stage 1")
			break
		}
	}
}
