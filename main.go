package main

import (
	"C"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/google/uuid"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/loggingingestion"
)

type Config struct {
	LogId    string
	Hostname string
}

type Plugin struct {
	Config Config
	Client loggingingestion.LoggingClient
}

var (
	plugins []*Plugin
)

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "oci_logging", "Oracle Cloud Infrastructure Logging Fluent Bit Plugin!")
}

//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("%v\n", err)
		return output.FLB_ERROR
	}
	config := Config{
		LogId:    output.FLBPluginConfigKey(ctx, "log_id"),
		Hostname: hostname,
	}
	if len(config.LogId) == 0 {
		fmt.Println("The log_id is a required value.")
		return output.FLB_ERROR
	}

	client, err := loggingingestion.NewLoggingClientWithConfigurationProvider(common.DefaultConfigProvider())
	if err != nil {
		fmt.Printf("%v\n", err)
		return output.FLB_ERROR
	}

	output.FLBPluginSetContext(ctx, len(plugins))
	plugins = append(plugins, &Plugin{
		Config: config,
		Client: client,
	})
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	var ret int
	var ts interface{}
	var record map[interface{}]interface{}
	var logEntryBatches []loggingingestion.LogEntryBatch
	var tagpath string

	dec := output.NewDecoder(data, int(length))
	plugin := plugins[output.FLBPluginGetContext(ctx).(int)]
	fluentTag := C.GoString(tag)
	logEntryBatchMap := make(map[string]loggingingestion.LogEntryBatch)

	if len(fluentTag) == 0 {
		tagpath = "empty"
	} else if strings.Contains(fluentTag, ".") {
		tagpath = fluentTag[strings.Index(fluentTag, ".")+1:]
	} else {
		tagpath = fluentTag
	}

	for {
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}
		var timestamp time.Time
		switch t := ts.(type) {
		case output.FLBTime:
			timestamp = ts.(output.FLBTime).Time
		case uint64:
			timestamp = time.Unix(int64(t), 0)
		default:
			timestamp = time.Now()
		}
		outputRecord := make(map[string]interface{})
		flatten("", record, outputRecord)
		data, err := json.Marshal(outputRecord)
		if err != nil {
			fmt.Printf("%v\n", err)
			continue
		}
		var sourceIdentifier string
		if val, ok := record["tailed_path"]; ok {
			sourceIdentifier = string(val.([]byte))
		} else {
			sourceIdentifier = ""
		}
		requestkey := tagpath + sourceIdentifier
		if val, ok := logEntryBatchMap[requestkey]; ok {
			val.Entries = append(val.Entries, loggingingestion.LogEntry{
				Data: common.String(string(data)),
				Id:   common.String(uuid.NewString()),
				Time: &common.SDKTime{Time: timestamp},
			})
		} else {
			logEntryBatchMap[requestkey] = loggingingestion.LogEntryBatch{
				Source:              common.String(plugin.Config.Hostname),
				Subject:             common.String(sourceIdentifier),
				Type:                common.String("com.oraclecloud.logging.custom." + tagpath),
				Defaultlogentrytime: &common.SDKTime{Time: timestamp},
				Entries: []loggingingestion.LogEntry{
					loggingingestion.LogEntry{
						Data: common.String(string(data)),
						Id:   common.String(uuid.NewString()),
						Time: &common.SDKTime{Time: timestamp},
					},
				},
			}
		}
	}
	for _, v := range logEntryBatchMap {
		logEntryBatches = append(logEntryBatches, v)
	}
	_, err := plugin.Client.PutLogs(context.Background(), loggingingestion.PutLogsRequest{
		LogId: common.String(plugin.Config.LogId),
		PutLogsDetails: loggingingestion.PutLogsDetails{
			LogEntryBatches: logEntryBatches,
			Specversion:     common.String("1.0"),
		},
	})
	if err != nil {
		fmt.Printf("%v\n", err)
		return output.FLB_RETRY
	}
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func flatten(prefix string, inputRecord map[interface{}]interface{}, outputRecord map[string]interface{}) {
	for k, v := range inputRecord {
		var key string
		if k.(string) == "log" {
			key = prefix + "msg"
		} else {
			key = prefix + k.(string)
		}
		switch value := v.(type) {
		case []byte:
			outputRecord[key] = string(value)
		case map[interface{}]interface{}:
			flatten(key+".", value, outputRecord)
		default:
			outputRecord[key] = value
		}
	}
}

func main() {
}
