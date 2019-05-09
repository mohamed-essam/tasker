package tasker

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
)

// ErrCyclicDependency caused by loading a task with cyclic dependencies that can't be resolved
var ErrCyclicDependency = errors.New("cyclic dependency in file")

type jsonTaskGroup struct {
	TaskGroupName  string        `json:"task_group_name"`
	ParameterCount int           `json:"parameter_count"`
	Subtasks       []jsonSubtask `json:"substasks"`
}

type jsonSubtask struct {
	TaskID       string    `json:"task_id"`
	SubtaskID    string    `json:"subtask_id"`
	Args         []jsonArg `json:"args"`
	Dependencies []string  `json:"dependencies"`
}

type jsonArg struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// LoadTaskGroupFromFile reads JSON definition of task group from file
func LoadTaskGroupFromFile(filename string) (TaskGroup, error) {
	var group jsonTaskGroup
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &group)
	if err != nil {
		return nil, err
	}

	subtaskMap := make(map[string]TaskInstance, 0)
	metadataMap := make(map[string]jsonSubtask, 0)

	createdCount := 0

	for createdCount < len(group.Subtasks) {
		lastCreatedCount := createdCount
		for _, subtask := range group.Subtasks {
			if _, ok := subtaskMap[subtask.SubtaskID]; ok { // subtask already created
				continue
			}

			task, err := manager().getTask(subtask.TaskID)
			if err != nil {
				return nil, err
			}

			taskInstance := task.New()

			satisfied := true
			for _, dependency := range subtask.Dependencies {
				if _, ok := subtaskMap[dependency]; !ok {
					satisfied = false
				}
			}

			if !satisfied {
				continue
			}

			for _, dependency := range subtask.Dependencies {
				parent := subtaskMap[dependency]
				taskInstance = taskInstance.DependsOn(parent)
			}

			subtaskMap[subtask.SubtaskID] = taskInstance
			metadataMap[subtask.SubtaskID] = subtask

			createdCount++
		}

		if createdCount == lastCreatedCount {
			return nil, ErrCyclicDependency
		}
	}

	taskGroup := newTaskGroup(subtaskMap, metadataMap, group.ParameterCount)

	return taskGroup, nil
}
