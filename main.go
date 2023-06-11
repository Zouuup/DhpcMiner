package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	common "github.com/ethereum/go-ethereum/common"
	types "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	ethclient "github.com/ethereum/go-ethereum/ethclient"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	// Importing the types package of your blog blockchain
	data "github.com/DhpcChain/Dhpc/x/data/types"

	request "github.com/DhpcChain/Dhpc/x/request/types"

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
			go m.processMinerPendingRecord(record)
		}

		time.Sleep(2 * time.Second)
	}
}

// Here start two goroutines: one to monitor miner_pending, another to monitor answer_pending

// Processes a record from miner_pending
func (m *Miner) processMinerPendingRecord(record request.RequestRecord) {
	m.mu.RLock()
	_, exist := m.responses[record.UUID]
	if exist {
		log.WithFields(logrus.Fields{"UUID": record.UUID}).Info("Record already processed")
		m.mu.RUnlock()
		return
	}
	m.responses[record.UUID] = &request.MinerResponse{}
	m.mu.RUnlock()

	log.WithFields(logrus.Fields{"UUID": record.UUID}).Info("Processing record")

	// Generate dataUsed
	dataQueryClient := data.NewQueryClient(m.client.Context())

	// getting records for teh address of user itself
	queryResp, err := dataQueryClient.DataAllByAddr(m.ctx, &data.QueryAllDataRequestByAddr{Address: record.Address})
	if err != nil {
		log.WithFields(logrus.Fields{"UUID": record.UUID, "Error": err}).Error("Error in getting data from data module")
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

	log.WithFields(logrus.Fields{"UUID": record.UUID, "Number of Data Used": len(dataUsedArray), "Answer": answer}).Info("Data used and answer calculated for user address itself")

	// TODO: the entire logs should only add dataused to dataused array
	// TODO: then, we move along and add all of them together and do the above once at the end

	// now, getting all the addresses user interacted with or they interacted with user
	logs, err := m.getLogs(record)
	if err != nil {
		log.WithFields(logrus.Fields{"UUID": record.UUID}).Error("Error in getting logs")
		return
	}

	log.WithFields(logrus.Fields{"UUID": record.UUID, "Number of EVM Logs": len(logs)}).Info("EVM Logs received")

	for _, logRecord := range logs {
		if len(logRecord.Topics) == 3 {
			var logAddress string
			localAnswer := int32(0)
			matchedRecord := int32(0)

			topic1Address := "0x" + logRecord.Topics[1].String()[26:]
			topic2Address := "0x" + logRecord.Topics[2].String()[26:]

			if strings.EqualFold(topic1Address, record.Address) {
				logAddress = topic2Address
			}
			if strings.EqualFold(topic2Address, record.Address) {
				logAddress = topic1Address
			}
			if logAddress == "0x0000000000000000000000000000000000000000" {
				// Use contract address instead
				logAddress = logRecord.Address.String()
			}

			// request data record of this address from DHPC
			queryResp, err := dataQueryClient.DataAllByAddr(m.ctx, &data.QueryAllDataRequestByAddr{Address: logAddress})
			if err != nil {
				log.WithFields(logrus.Fields{"UUID": record.UUID}).Error("Error in getting data from data module")
				continue
			}

			log.WithFields(logrus.Fields{"UUID": record.UUID, "EVM Address": logAddress, "Count": len(queryResp.Data)}).Info("Looking up matching data records for EVM address")

			// now, iterate over all the data records and see if they match
			for _, data := range queryResp.Data {
				log.WithFields(logrus.Fields{"UUID": record.UUID, "Data Hash": data.Hash, "EVM Address": logAddress, "Method": data.Event}).Info("Processing data record for EVM log")

				// check if method matches
				if crypto.Keccak256Hash([]byte(data.Event)) == logRecord.Topics[0] || data.Event == "any" {
					log.WithFields(logrus.Fields{"UUID": record.UUID, "Data Hash": data.Hash, "EVM Address": logAddress, "Data Method": data.Event, "EVMlog Method": logRecord.Topics[0].Hex()}).Info("Method matched")

					// check if block number is within block validity
					if record.Block-data.BlockValidity < logRecord.BlockNumber || data.BlockValidity == 0 {
						log.WithFields(logrus.Fields{"UUID": record.UUID, "Data Hash": data.Hash, "EVM Address": logAddress,
							"Data Method": data.Event, "EVMlog Method": logRecord.Topics[0].Hex(), "Block": record.Block,
							"BlockValidity": data.BlockValidity, "EVMlog Block": logRecord.BlockNumber}).Info("Action within block validity")

						dataUsedArray = append(dataUsedArray, data.Hash)
						localAnswer = localAnswer + data.Score
						matchedRecord = matchedRecord + 1
					}
				}
			}
			log.WithFields(logrus.Fields{"UUID": record.UUID, "Number of matched records": matchedRecord, "Local Answer": localAnswer}).Info("Data record processed")
			if matchedRecord > 0 {
				answer = answer + (localAnswer / matchedRecord)

			}
		}
	}

	// make sure dataUsed is all unique
	dataUsed := m.uniqueStrings(dataUsedArray)

	// Generate a random salt, make sure it's always positive
	salt := int32(rand.Intn(100000))

	// Generate sumStr
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
func (m *Miner) getLogs(record request.RequestRecord) ([]types.Log, error) {
	// Step 1, find if we have an RPC for the given network, we can detect that by looking at network field of the request and looking up environment variables with same name + _RPC
	// check if networkname + _RPC is set in the environment variables
	rpc_address := os.Getenv(record.Network + "_RPC")
	if rpc_address == "" {
		log.WithFields(logrus.Fields{"UUID": record.UUID, "Network": record.Network}).Error("RPC address not found in environment variables, looked for " + record.Network + "_RPC")
		return nil, errors.New("RPC address not found in environment variables")
	}

	// now we have the RPC address, we can connect to the RPC and get the data
	ethClient, err := ethclient.Dial(rpc_address)
	if err != nil {
		log.WithFields(logrus.Fields{"UUID": record.UUID, "Network": record.Network, "RPC": rpc_address}).Error("Error in connecting to RPC")
		return nil, errors.New("error in connecting to RPC")
	}

	address := common.HexToAddress(record.Address)

	query := ethereum.FilterQuery{
		FromBlock: nil,
		ToBlock:   big.NewInt(int64(record.Block)),
		Addresses: []common.Address{},
		Topics: [][]common.Hash{
			nil,
			{address.Hash()},
			nil,
		},
	}
	logs, err := ethClient.FilterLogs(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("error retrieving logs: %v", err)
	}

	query = ethereum.FilterQuery{
		FromBlock: nil,
		ToBlock:   big.NewInt(int64(record.Block)),
		Addresses: []common.Address{},
		Topics: [][]common.Hash{
			nil,
			nil,
			{address.Hash()},
		},
	}
	logs2, err := ethClient.FilterLogs(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("error retrieving logs: %v", err)
	}

	logs = append(logs, logs2...)

	return logs, nil
}

func (m *Miner) uniqueStrings(strs []string) string {
	uniqueMap := make(map[string]bool)
	var uniqueArr []string

	for _, str := range strs {
		if !uniqueMap[str] {
			uniqueMap[str] = true
			uniqueArr = append(uniqueArr, str)
		}
	}

	return strings.Join(uniqueArr, ",")
}
