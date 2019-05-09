package tasker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"time"

	redis "github.com/go-redis/redis"
)

// ErrorHandler is a function that handles processing errors and returns whether to continue processing or panic
type ErrorHandler func(error) bool

var _taskManagerInstance taskManager

// ErrRedefined caused by registering same Task ID twice
var ErrRedefined = errors.New("task redefenition")

// ErrTaskNotFound caused by loading a task that was not registered
var ErrTaskNotFound = errors.New("task not found")

var errNoTasks = errors.New("no runnable tasks found")

type taskManager interface {
	registerTask(task Task) error
	processOnce() error
	getTask(taskID string) (Task, error)
}

func manager() taskManager {
	if _taskManagerInstance == nil {
		_taskManagerInstance = &taskManagerC{
			tasks: make(map[string]Task, 0),
		}
	}

	return _taskManagerInstance
}

type taskManagerC struct {
	tasks map[string]Task
}

func (tm *taskManagerC) registerTask(task Task) error {
	if _, ok := tm.tasks[task.getID()]; ok {
		return ErrRedefined
	}
	tm.tasks[task.getID()] = task
	return nil
}

func (tm *taskManagerC) getTask(taskID string) (Task, error) {
	if value, ok := tm.tasks[taskID]; ok {
		return value, nil
	}

	return nil, ErrTaskNotFound
}

func (tm *taskManagerC) processOnce() error {
	taskData, err := redisClient().BRPop(time.Second, readySetID()).Result()
	if err != nil {
		if netErr, ok := err.(net.Error); ok || err == redis.Nil {
			if netErr.Timeout() {
				return nil
			}
		}
		return err
	}

	if len(taskData) < 2 {
		return nil
	}

	taskBytes := []byte(taskData[1])
	var task *taskInstanceC
	err = json.Unmarshal(taskBytes, &task)
	if err != nil {
		return err
	}

	runner, err := tm.getTask(task.TaskID)
	if err != nil {
		return err
	}

	err = runner.run(task)

	if err != nil {
		return err
	}

	for _, dependent := range task.Dependents {
		lockDependent(dependent)
		dependentValue, err := redisClient().HGet(waitSetID(), dependent).Result()
		retryCount := 0
		for err != nil && retryCount < 5 {
			dependentValue, err = redisClient().HGet(waitSetID(), dependent).Result()
			retryCount++
		}
		retryCount = 0
		for redisClient().HDel(waitSetID(), dependent).Err() != nil && retryCount < 5 {
			retryCount++
		}
		dependentBytes := []byte(dependentValue)
		var dependentTask *taskInstanceC
		err = json.Unmarshal(dependentBytes, &dependentTask)
		if err != nil {
			unlockDependent(dependent)
			log.Print(err)
			continue
		}

		dependentTask.DependencyCount--
		dependentTask.Schedule()

		unlockDependent(dependent)
	}

	return nil
}

func lockDependent(taskID string) error {
	ok := false
	for !ok {
		var err error
		ok, err = redisClient().SetNX(lockKeyID(taskID), 1, 3*time.Second).Result()
		if err != nil {
			return err
		}
	}

	return nil
}

func unlockDependent(taskID string) error {
	return redisClient().Del(lockKeyID(taskID)).Err()
}

// StartSync starts processing jobs in foreground
func StartSync() error {
	for {
		err := manager().processOnce()
		if err != nil {
			return err
		}
	}
}

// StartAsync starts processing jobs in background without an error handler
// this will panic on any error from processing
func StartAsync(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			return
		default:
			err := manager().processOnce()
			if err != nil {
				panic(err)
			}
		}
	}()
}

// StartAsyncWithHandler starts processing jobs in background and calls error handler func on any error
// the error might originate from the task runner or tasker itself
func StartAsyncWithHandler(ctx context.Context, handler ErrorHandler) {
	go func() {
		select {
		case <-ctx.Done():
			return
		default:
			err := manager().processOnce()
			if err != nil {
				handled := handler(err)
				if !handled {
					panic(err)
				}
			}
		}
	}()
}
