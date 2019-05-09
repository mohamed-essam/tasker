package tasker

import "errors"

// ErrParameterMismatch caused by sending a number of parameters not matching to the taskgroup definition
var ErrParameterMismatch = errors.New("parameter count mismatch")

// ErrInvalidParam caused by a parameter type not matching schema
var ErrInvalidParam = errors.New("invalid parameter type")

// TaskGroup defines a group of tasks with inter-dependencies
type TaskGroup interface {
	Run() error
	SetParameters(params ...interface{}) error
}

type taskGroupC struct {
	tasks          map[string]TaskInstance
	metadata       map[string]jsonSubtask
	parameterCount int
}

func (tg *taskGroupC) Run() error {
	for _, task := range tg.tasks {
		if task.dependencyCount() != 0 {
			task.Schedule()
		}
	}

	for _, task := range tg.tasks {
		if task.dependencyCount() == 0 {
			task.Schedule()
		}
	}

	return nil
}

func (tg *taskGroupC) SetParameters(params ...interface{}) error {
	if len(params) != tg.parameterCount {
		return ErrParameterMismatch
	}

	for idx, task := range tg.tasks {
		metadata := tg.metadata[idx]

		args := make([]interface{}, len(metadata.Args))
		for paramIdx, param := range metadata.Args {
			if param.Type == "static" {
				args[paramIdx] = param.Value
			} else if param.Type == "parameter" {
				val, ok := param.Value.(float64)
				if !ok {
					return ErrInvalidParam
				}
				args[paramIdx] = params[int(val)]
			} else {
				return ErrInvalidParam
			}
		}

		tg.tasks[idx] = task.WithArgs(args...)
	}

	return nil
}

func newTaskGroup(taskMap map[string]TaskInstance, metadataMap map[string]jsonSubtask, parameterCount int) TaskGroup {
	return &taskGroupC{
		tasks:          taskMap,
		metadata:       metadataMap,
		parameterCount: parameterCount,
	}
}
