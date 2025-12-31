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

func GetEnvDefaultBool(key string, defval bool) bool {
	env := os.Getenv(key)
	if env == "" {
		return defval
	}
	return env == "true"
}

func GetEnvDefaultFloat(key string, defval float64) float64 {
	env := os.Getenv(key)
	if env == "" {
		return defval
	}
	val, err := strconv.ParseFloat(env, 64)
	if err != nil {
		return defval
	}
	// Validate ratio is between 0 and 1
	if val <= 0 || val > 1 {
		return defval
	}
	return val
}

var EnvEnableDelayedScalingNamespace string = GetEnvDefaultString("ENABLE_DELAYED_SCALING_NAMESPACE", "tmp-test-dev-3,atms-oversea-test,lwd-test2,ai-app-test,ai-nlp-llm-test")

var EnvEnableDelayedScalingAllTestNamespace bool = GetEnvDefaultBool("ENABLE_DELAYED_SCALING_ALL_TEST_NAMESPACE", false)

var EnvDelayedScalingTimeoutSecond int = GetEnvDefaultInt("DELAYED_SCALING_TIMEOUT_SECOND", 300)

var EnvDelayedScalingSleepDurationSecond int = GetEnvDefaultInt("DELAYED_SCALING_SLEEP_DURATION_SECOND", 20)

var EnvScaleUpThresholdRatio float64 = GetEnvDefaultFloat("SCALE_UP_THRESHOLD_RATIO", 0.5)

var EnvSchedulerDebounceWindowSecond int = GetEnvDefaultInt("SCHEDULER_DEBOUNCE_WINDOW_SECOND", 10)
