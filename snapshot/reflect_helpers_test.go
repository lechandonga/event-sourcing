package snapshot_test

// 测试需要 FileStore 的目录；FileStore 提供 DirForTest 测试访问器。
// 损坏注入：直接覆写快照 JSON 文件的 payload 字段模拟磁盘位翻转。
