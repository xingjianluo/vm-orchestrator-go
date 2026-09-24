package backend

import "errors"

// errBackendNotDefined 表示 Start 时后端尚不存在实例定义。
var errBackendNotDefined = errors.New("backend: instance not defined")

// errInjected 表示故障注入导致的后端失败。
var errInjected = errors.New("backend: injected fault")
