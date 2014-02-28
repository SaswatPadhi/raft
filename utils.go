package raft

import (
	"math/rand"

	"log"
)

type LOG_TYPE struct {
	priority int8
	name     string
}

var (
	INFO = LOG_TYPE{0, "[INFO]"}
	WARN = LOG_TYPE{1, "[WARN]"}
	EROR = LOG_TYPE{2, "[EROR]"}
	NONE = LOG_TYPE{3, "[NONE]"}

	LOG_FLAG = EROR
)

func (LOG_LEVL LOG_TYPE) Println(a ...interface{}) {
	if LOG_LEVL.priority >= LOG_FLAG.priority {
		log.Println(LOG_LEVL.name, a)
	}
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func randIntDev(mean int, delta float32) int {
	return randInt(int(float32(mean)*(1.0-delta)), int(float32(mean)*(1.0+delta)))
}
