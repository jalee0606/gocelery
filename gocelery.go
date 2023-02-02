// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"strings"
	"time"
)

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
	broker  CeleryBroker
	backend CeleryBackend
	worker  *CeleryWorker
}

// CeleryBroker is interface for celery broker database
type CeleryBroker interface {
	SendCeleryMessage(*CeleryMessage) error
	SendCeleryMessageToQueue(*CeleryMessage, string) error
	GetTaskMessage() (*TaskMessage, error) // must be non-blocking
	SendCeleryMessageV2(*CeleryMessageV2, string) error
}

// CeleryBackend is interface for celery backend database
type CeleryBackend interface {
	GetResult(string) (*ResultMessage, error) // must be non-blocking
	SetResult(taskID string, result *ResultMessage) error
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker, backend CeleryBackend, numWorkers int, options ...Option) (*CeleryClient, error) {
	c := Config{}
	for _, opt := range options {
		opt(&c)
	}
	return &CeleryClient{
		broker,
		backend,
		NewCeleryWorker(broker, backend, numWorkers, &c),
	}, nil
}

// Register task
func (cc *CeleryClient) Register(name string, task interface{}) {
	cc.worker.Register(name, task)
}

// StartWorkerWithContext starts celery workers with given parent context
func (cc *CeleryClient) StartWorkerWithContext(ctx context.Context) {
	cc.worker.StartWorkerWithContext(ctx)
}

// StartWorker starts celery workers
func (cc *CeleryClient) StartWorker() {
	cc.worker.StartWorker()
}

// StopWorker stops celery workers
func (cc *CeleryClient) StopWorker() {
	cc.worker.StopWorker()
}

// WaitForStopWorker waits for celery workers to terminate
func (cc *CeleryClient) WaitForStopWorker() {
	cc.worker.StopWait()
}

// Delay gets asynchronous result
func (cc *CeleryClient) Delay(task string, args ...interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Args = args
	return cc.delay(celeryTask)
}

func (cc *CeleryClient) DelayToQueue(task string, queueName string, args ...interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Args = args
	return cc.delayToQueue(celeryTask, queueName)
}

func (cc *CeleryClient) DelayToQueueV2(task string, queueName string, args ...interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Args = args
	return cc.delayToQueueV2(celeryTask, queueName)
}

// DelayKwargs gets asynchronous results with argument map
func (cc *CeleryClient) DelayKwargs(task string, args map[string]interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Kwargs = args
	return cc.delay(celeryTask)
}

// DelayKwargs gets asynchronous results with argument map
func (cc *CeleryClient) DelayKwargsToQueue(task string, queueName string, args map[string]interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Kwargs = args
	return cc.delayToQueue(celeryTask, queueName)
}

func (cc *CeleryClient) delay(task *TaskMessage) (*AsyncResult, error) {
	defer releaseTaskMessage(task)
	encodedMessage, err := task.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := getCeleryMessage(encodedMessage)
	defer releaseCeleryMessage(celeryMessage)
	err = cc.broker.SendCeleryMessage(celeryMessage)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		TaskID:  task.ID,
		backend: cc.backend,
	}, nil
}

func repr(args []interface{}) string {
	b := new(bytes.Buffer)
	b.WriteRune('(')
	js := json.NewEncoder(b)
	for _, obj := range args {
		js.Encode(obj)
		b.WriteRune(',')
	}
	b.Truncate(b.Len() - 2)
	b.WriteRune(')')
	return strings.ReplaceAll(string(b.Bytes()), "\n", "")
}

func (cc *CeleryClient) delayToQueueV2(task *TaskMessage, queue string) (*AsyncResult, error) {
	defer releaseTaskMessage(task)
	kwargs, err := json.Marshal(task.Kwargs)
	header := HeaderV2{
		Language:   "py",
		TaskName:   task.Task,
		Id:         task.ID,
		RootId:     uuid.Must(uuid.NewV4()).String(),
		Group:      uuid.Must(uuid.NewV4()).String(),
		ParentId:   uuid.Must(uuid.NewV4()).String(),
		Expires:    task.Expires,
		Eta:        task.ETA,
		Retries:    task.Retries,
		KwargsRepr: string(kwargs),
		ArgsRepr:   repr(task.Args),
	}
	body := BodyV2{
		args:   task.Args,
		kwargs: task.Kwargs,
	}
	celeryMessage := getCeleryMessageV2(header, body)
	defer releaseCeleryMessageV2(celeryMessage)
	err = cc.broker.SendCeleryMessageV2(celeryMessage, queue)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		TaskID:  task.ID,
		backend: cc.backend,
	}, nil
}

func (cc *CeleryClient) delayToQueue(task *TaskMessage, queue string) (*AsyncResult, error) {
	defer releaseTaskMessage(task)
	encodedMessage, err := task.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := getCeleryMessage(encodedMessage)
	defer releaseCeleryMessage(celeryMessage)
	err = cc.broker.SendCeleryMessageToQueue(celeryMessage, queue)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		TaskID:  task.ID,
		backend: cc.backend,
	}, nil
}

// CeleryTask is an interface that represents actual task
// Passing CeleryTask interface instead of function pointer
// avoids reflection and may have performance gain.
// ResultMessage must be obtained using GetResultMessage()
type CeleryTask interface {

	// ParseKwargs - define a method to parse kwargs
	ParseKwargs(map[string]interface{}) error

	// RunTask - define a method for execution
	RunTask() (interface{}, error)
}

// AsyncResult represents pending result
type AsyncResult struct {
	TaskID  string
	backend CeleryBackend
	result  *ResultMessage
}

// Get gets actual result from backend
// It blocks for period of time set by timeout and returns error if unavailable
func (ar *AsyncResult) Get(timeout time.Duration) (interface{}, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-timeoutChan:
			err := fmt.Errorf("%v timeout getting result for %s", timeout, ar.TaskID)
			return nil, err
		case <-ticker.C:
			val, err := ar.AsyncGet()
			if err != nil {
				continue
			}
			return val, nil
		}
	}
}

// AsyncGet gets actual result from backend and returns nil if not available
func (ar *AsyncResult) AsyncGet() (interface{}, error) {
	if ar.result != nil {
		return ar.result.Result, nil
	}
	val, err := ar.backend.GetResult(ar.TaskID)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, err
	}
	if val.Status != "SUCCESS" {
		return nil, fmt.Errorf("error response status %v", val)
	}
	ar.result = val
	return val.Result, nil
}

// Ready checks if actual result is ready
func (ar *AsyncResult) Ready() (bool, error) {
	if ar.result != nil {
		return true, nil
	}
	val, err := ar.backend.GetResult(ar.TaskID)
	if err != nil {
		return false, err
	}
	ar.result = val
	return (val != nil), nil
}
