package tasker

import (
	"github.com/google/uuid"
)

// Runner defines a function to run a specific task
type Runner func(...interface{}) error

// Task defines a standalone task that can be run
type Task interface {
	New() TaskInstance
	getID() string
	run(ti TaskInstance) error
}

type taskC struct {
	ID     string
	runner Runner
}

// RegisterTask registers a task ID handler
func RegisterTask(id string, runner Runner) (Task, error) {
	ret := &taskC{
		ID:     id,
		runner: runner,
	}

	err := manager().registerTask(ret)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// New creates a new instance of the task but does not enqueue it
func (t taskC) New() TaskInstance {
	return &taskInstanceC{
		TaskID:          t.ID,
		Args:            make([]interface{}, 0),
		UUID:            uuid.New().String(),
		Dependents:      make([]string, 0),
		DependencyCount: 0,
	}
}

func (t taskC) getID() string {
	return t.ID
}

func (t taskC) run(ti TaskInstance) error {
	args := ti.getArgs()
	err := t.runner(args...)
	return err
}

// TaskInstance defines a serializable job
type TaskInstance interface {
	WithArgs(args ...interface{}) TaskInstance
	DependsOn(dep TaskInstance) TaskInstance
	Schedule() error
	getUUID() string
	addDependent(TaskInstance)
	dependencyCount() int
	getArgs() []interface{}
}

type taskInstanceC struct {
	TaskID          string        `json:"task_id"`
	Args            []interface{} `json:"args"`
	UUID            string        `json:"uuid"`
	Dependents      []string      `json:"dependents"`
	DependencyCount int           `json:"dependency_count"`
}

// WithArgs serializes arguments with job
func (ti *taskInstanceC) WithArgs(args ...interface{}) TaskInstance {
	ti.Args = args
	return ti
}

// DependsOn adds a dependency on another task instance
// this instance will not run before the other task is completed
func (ti *taskInstanceC) DependsOn(dep TaskInstance) TaskInstance {
	ti.DependencyCount++
	dep.addDependent(ti)
	return ti
}

func (ti *taskInstanceC) getUUID() string {
	return ti.UUID
}

func (ti *taskInstanceC) addDependent(t TaskInstance) {
	ti.Dependents = append(ti.Dependents, t.getUUID())
}

// Schedule enqueues the job to be run as soon as all requirements are met
func (ti *taskInstanceC) Schedule() error {
	if ti.DependencyCount == 0 {
		return redisClient().LPush(readySetID(), ti).Err()
	}
	return redisClient().HSet(waitSetID(), ti.UUID, ti).Err()
}

func (ti *taskInstanceC) dependencyCount() int {
	return ti.DependencyCount
}

func (ti *taskInstanceC) getArgs() []interface{} {
	return ti.Args
}
