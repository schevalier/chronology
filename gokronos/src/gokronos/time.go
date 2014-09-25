package gokronos

import (
	"time"
)

type KronosTime struct {
	Time int64 // Unix() * 1e7
}

/*
	Helper function which returns the current timestamp, Kronos style
*/
func KronosTimeNow() *KronosTime {
	return TimetoKronosTime(time.Now())
}

func TimetoKronosTime(date time.Time) *KronosTime {
	return &KronosTime{date.Unix() * 1e7}
}
