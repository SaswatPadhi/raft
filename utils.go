package raft

import (
	"math/rand"

	"log"
)

type DBG_TYPE struct {
	priority int8
	name     string
}

var (
	DBG_INFO = DBG_TYPE{0, "[INFO]"}
	DBG_WARN = DBG_TYPE{1, "[WARN]"}
	DBG_EROR = DBG_TYPE{2, "[EROR]"}
	DBG_NONE = DBG_TYPE{3, "[NONE]"}

	DBG_FLAG = DBG_EROR
)

func (DBG_LEVL DBG_TYPE) Println(a ...interface{}) {
	if DBG_LEVL.priority >= DBG_FLAG.priority {
		log.Println(DBG_LEVL.name, a)
	}
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func randIntDev(mean int, delta float32) int {
	return randInt(int(float32(mean)*(1.0-delta)), int(float32(mean)*(1.0+delta)))
}
