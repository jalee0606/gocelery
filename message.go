// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

type PropertiesV2 struct {
	CorrelationId string `json:"correlation_id"`
	ContentType   string `json:"content_type"`
	Encoding      string `json:"content_encoding"`
	//Optional
	ReplyTo string `json:"reply_to,omitempty"`
}

type BodyV2 struct {
	args   []any
	kwargs map[string]any
}

type HeaderV2 struct {
	Language string `json:"lang"`
	TaskName string `json:"task"`
	Id       string `json:"task_id"`
	RootId   string `json:"root_id"`
	ParentId string `json:"parent_id"`
	Group    string `json:"group_id"`
	// Optional
	MethodName  string     `json:"method_name,omitempty"`
	AliasName   string     `json:"alias_name,omitempty"`
	Eta         *string    `json:"ETA,omitempty"`
	Expires     *time.Time `json:"expires,omitempty"`
	Retries     int        `json:"retries,omitempty"`
	TimeLimit   string     `json:"timelimit,omitempty"`
	ArgsRepr    string     `json:"argsrepr,omitempty"`
	KwargsRepr  string     `json:"kwargsrepr,omitempty"`
	Origin      string     `json:"origin,omitempty"`
	ReplaceTask int        `json:"replaced_task_nesting,omitempty"`
}

type CeleryMessageV2 struct {
	Body       BodyV2       `json:"body"`
	Headers    HeaderV2     `json:"headers"`
	Properties PropertiesV2 `json:"properties"`
}

// CeleryMessage is actual message to be sent to Redis
type CeleryMessage struct {
	Body            string                 `json:"body"`
	Headers         map[string]interface{} `json:"headers,omitempty"`
	ContentType     string                 `json:"content-type"`
	Properties      CeleryProperties       `json:"properties"`
	ContentEncoding string                 `json:"content-encoding"`
}

const (
	jsonV2Opt = `{"callbacks":null,"errbacks":null,"chain":null,"chord":null}`
)

func (cm *CeleryMessageV2) GetBody() (string, error) {
	b := new(bytes.Buffer)
	b.WriteRune('[')
	js := json.NewEncoder(b)
	if cm.Body.args == nil {
		b.WriteString("[]")
	} else if err := js.Encode(cm.Body.args); err != nil {
		return "", fmt.Errorf("args json encode: %w", err)
	}
	b.WriteRune(',')
	if cm.Body.kwargs == nil {
		b.WriteString("{}")
	} else if err := js.Encode(cm.Body.kwargs); err != nil {
		return "", fmt.Errorf("kwargs json encode: %w", err)
	}
	b.WriteRune(',')
	b.WriteString(jsonV2Opt)
	b.WriteRune(']')
	strWithoutNewLine := strings.ReplaceAll(string(b.Bytes()), "\n", "")
	//fmt.Printf("%s\n", strWithoutNewLine)
	return base64.StdEncoding.EncodeToString([]byte(strWithoutNewLine)), nil
}

func (cm *CeleryMessageV2) GetHeader() (map[string]interface{}, error) {
	headers := make(map[string]interface{})
	jsonStr, err := json.Marshal(cm.Headers)
	if err != nil {
		return headers, fmt.Errorf("error marshaling header: %w", err)
	}
	err = json.Unmarshal(jsonStr, &headers)
	if err != nil {
		return headers, fmt.Errorf("error unmarshaling into map: %w", err)
	}
	return headers, nil
}

func (cm *CeleryMessage) reset() {
	cm.Headers = nil
	cm.Body = ""
	cm.Properties.CorrelationID = uuid.Must(uuid.NewV4()).String()
	cm.Properties.ReplyTo = uuid.Must(uuid.NewV4()).String()
	cm.Properties.DeliveryTag = uuid.Must(uuid.NewV4()).String()
}

func (cm *CeleryMessageV2) reset() {
	cm.Headers = HeaderV2{}
	cm.Body = BodyV2{}
	cm.Properties = PropertiesV2{
		CorrelationId: uuid.Must(uuid.NewV4()).String(),
		ContentType:   "application/json",
		Encoding:      "utf-8",
	}
}

var celeryMessagePoolV2 = sync.Pool{
	New: func() interface{} {
		return &CeleryMessageV2{
			Properties: PropertiesV2{
				CorrelationId: uuid.Must(uuid.NewV4()).String(),
				ContentType:   "application/json",
				Encoding:      "utf-8",
			},
		}
	},
}

func getCeleryMessageV2(header HeaderV2, body BodyV2) *CeleryMessageV2 {
	msg := celeryMessagePoolV2.Get().(*CeleryMessageV2)
	msg.Headers = header
	msg.Body = body
	return msg
}

var celeryMessagePool = sync.Pool{
	New: func() interface{} {
		return &CeleryMessage{
			Body:        "",
			Headers:     nil,
			ContentType: "application/json",
			Properties: CeleryProperties{
				BodyEncoding:  "base64",
				CorrelationID: uuid.Must(uuid.NewV4()).String(),
				ReplyTo:       uuid.Must(uuid.NewV4()).String(),
				DeliveryInfo: CeleryDeliveryInfo{
					Priority:   255,
					RoutingKey: "celery",
					Exchange:   "celery",
				},
				DeliveryMode: 2,
				DeliveryTag:  uuid.Must(uuid.NewV4()).String(),
			},
			ContentEncoding: "utf-8",
		}
	},
}

func getCeleryMessage(encodedTaskMessage string) *CeleryMessage {
	msg := celeryMessagePool.Get().(*CeleryMessage)
	msg.Body = encodedTaskMessage
	return msg
}

func releaseCeleryMessage(v *CeleryMessage) {
	v.reset()
	celeryMessagePool.Put(v)
}

func releaseCeleryMessageV2(v *CeleryMessageV2) {
	v.reset()
	celeryMessagePoolV2.Put(v)
}

// CeleryProperties represents properties json
type CeleryProperties struct {
	BodyEncoding  string             `json:"body_encoding"`
	CorrelationID string             `json:"correlation_id"`
	ReplyTo       string             `json:"reply_to"`
	DeliveryInfo  CeleryDeliveryInfo `json:"delivery_info"`
	DeliveryMode  int                `json:"delivery_mode"`
	DeliveryTag   string             `json:"delivery_tag"`
}

// CeleryDeliveryInfo represents deliveryinfo json
type CeleryDeliveryInfo struct {
	Priority   int    `json:"priority"`
	RoutingKey string `json:"routing_key"`
	Exchange   string `json:"exchange"`
}

// GetTaskMessage retrieve and decode task messages from broker
func (cm *CeleryMessage) GetTaskMessage() *TaskMessage {
	// ensure content-type is 'application/json'
	if cm.ContentType != "application/json" {
		log.Println("unsupported content type " + cm.ContentType)
		return nil
	}
	// ensure body encoding is base64
	if cm.Properties.BodyEncoding != "base64" {
		log.Println("unsupported body encoding " + cm.Properties.BodyEncoding)
		return nil
	}
	// ensure content encoding is utf-8
	if cm.ContentEncoding != "utf-8" {
		log.Println("unsupported encoding " + cm.ContentEncoding)
		return nil
	}
	// decode body
	taskMessage, err := DecodeTaskMessage(cm.Body)
	if err != nil {
		log.Println("failed to decode task message")
		return nil
	}
	return taskMessage
}

// TaskMessage is celery-compatible message
type TaskMessage struct {
	ID      string                 `json:"id"`
	Task    string                 `json:"task"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Retries int                    `json:"retries"`
	ETA     *string                `json:"eta"`
	Expires *time.Time             `json:"expires"`
}

func (tm *TaskMessage) reset() {
	tm.ID = uuid.Must(uuid.NewV4()).String()
	tm.Task = ""
	tm.Args = nil
	tm.Kwargs = nil
}

var taskMessagePool = sync.Pool{
	New: func() interface{} {
		eta := time.Now().Format(time.RFC3339)
		return &TaskMessage{
			ID:      uuid.Must(uuid.NewV4()).String(),
			Retries: 0,
			Kwargs:  nil,
			ETA:     &eta,
		}
	},
}

func getTaskMessage(task string) *TaskMessage {
	msg := taskMessagePool.Get().(*TaskMessage)
	msg.Task = task
	msg.Args = make([]interface{}, 0)
	msg.Kwargs = make(map[string]interface{})
	msg.ETA = nil
	return msg
}

func releaseTaskMessage(v *TaskMessage) {
	v.reset()
	taskMessagePool.Put(v)
}

// DecodeTaskMessage decodes base64 encrypted body and return TaskMessage object
func DecodeTaskMessage(encodedBody string) (*TaskMessage, error) {
	body, err := base64.StdEncoding.DecodeString(encodedBody)
	if err != nil {
		return nil, err
	}
	message := taskMessagePool.Get().(*TaskMessage)
	err = json.Unmarshal(body, message)
	if err != nil {
		return nil, err
	}
	return message, nil
}

// Encode returns base64 json encoded string
func (tm *TaskMessage) Encode() (string, error) {
	if tm.Args == nil {
		tm.Args = make([]interface{}, 0)
	}
	jsonData, err := json.Marshal(tm)
	if err != nil {
		return "", err
	}
	encodedData := base64.StdEncoding.EncodeToString(jsonData)
	return encodedData, err
}

// ResultMessage is return message received from broker
type ResultMessage struct {
	ID        string        `json:"task_id"`
	Status    string        `json:"status"`
	Traceback interface{}   `json:"traceback"`
	Result    interface{}   `json:"result"`
	Children  []interface{} `json:"children"`
}

func (rm *ResultMessage) reset() {
	rm.Result = nil
}

var resultMessagePool = sync.Pool{
	New: func() interface{} {
		return &ResultMessage{
			Status:    "SUCCESS",
			Traceback: nil,
			Children:  nil,
		}
	},
}

func getResultMessage(val interface{}) *ResultMessage {
	msg := resultMessagePool.Get().(*ResultMessage)
	msg.Result = val
	return msg
}

func getReflectionResultMessage(val *reflect.Value) *ResultMessage {
	msg := resultMessagePool.Get().(*ResultMessage)
	msg.Result = GetRealValue(val)
	return msg
}

func releaseResultMessage(v *ResultMessage) {
	v.reset()
	resultMessagePool.Put(v)
}
