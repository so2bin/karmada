package binding

import (
	"errors"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

func loadEnv() error {
	// load env from .env
	var err error
	if err = godotenv.Load(); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			panic(err)
		}
	}
	return nil
}

var _ = loadEnv()

func GetEnvDefaultString(key string, defval string) string {
	env := os.Getenv(key)
	if env == "" {
		return defval
	}
	return env
}

func GetEnvDefaultInt(key string, defval int) int {
	env := os.Getenv(key)
	if env == "" {
		return defval
	}
	val, err := strconv.Atoi(env)
	if err != nil {
		return defval
	}
	return val
}

var EnvEnableDelayedScalingNamespace string = GetEnvDefaultString("ENABLE_DELAYED_SCALING_NAMESPACE", "tmp-test-dev-3,lwd-test2")

var EnvDelayedScalingTimeoutSecond int = GetEnvDefaultInt("DELAYED_SCALING_TIMEOUT_SECOND", 1800)

var EnvDelayedScalingSleepDurationSecond int = GetEnvDefaultInt("DELAYED_SCALING_SLEEP_DURATION_SECOND", 20)
