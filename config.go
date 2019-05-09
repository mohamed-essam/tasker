package tasker

import "fmt"

// Cfg holds configurations for tasker
type Cfg struct {
	TaskerID  string
	RedisHost string
	RedisPort string
	RedisDB   int
}

var _config *Cfg

func config() *Cfg {
	if _config == nil {
		_config = &Cfg{
			TaskerID:  "tasker",
			RedisHost: "127.0.0.1",
			RedisPort: "6379",
			RedisDB:   0,
		}
	}

	return _config
}

// Configure sets configurations for tasker
func Configure(config Cfg) {
	_config = &config
}

func readySetID() string {
	return fmt.Sprintf("tasker:%s:jobs:ready", config().TaskerID)
}

func waitSetID() string {
	return fmt.Sprintf("tasker:%s:jobs", config().TaskerID)
}

func lockKeyID(taskID string) string {
	return fmt.Sprintf("tasker:%s:lock:%s", config().TaskerID, taskID)
}
