package eventstore_test

import (
	"os"
	"time"
)

func timeAfter() <-chan time.Time { return time.After(time.Second) }

func openAppend(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
}

func writeAll(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}

func appendAll(path string, data []byte) error {
	f, err := openAppend(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	return err
}
