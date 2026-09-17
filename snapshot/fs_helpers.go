package snapshot

import "os"

func readFile(path string) ([]byte, error) { return os.ReadFile(path) }
func removeFile(path string) error         { return os.Remove(path) }
