package model

import (
	"errors"
	"fmt"
)

// 可区分的哨兵错误：调用方可通过 errors.Is 判定拒绝原因。
var (
	// ErrNotFound：实例不存在。
	ErrNotFound = errors.New("instance not found")
	// ErrAlreadyExists：同名/同ID 实例已存在。
	ErrAlreadyExists = errors.New("instance already exists")
	// ErrInvalidTransition：状态迁移非法。
	ErrInvalidTransition = errors.New("invalid state transition")
	// ErrQuotaExceeded：宿主资源不足，准入被拒绝。
	ErrQuotaExceeded = errors.New("host quota exceeded")
	// ErrBackendFailed：后端执行失败。
	ErrBackendFailed = errors.New("backend operation failed")
	// ErrInconsistent：实例处于人工介入状态，禁止常规操作。
	ErrInconsistent = errors.New("instance marked inconsistent, manual intervention required")
	// ErrInvalidArgument：参数非法。
	ErrInvalidArgument = errors.New("invalid argument")
)

// TransitionError 携带非法迁移的上下文：当前状态与目标操作。
type TransitionError struct {
	ID   string
	From State
	Op   OpType
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("invalid transition: instance=%s op=%s from=%s", e.ID, e.Op, e.From)
}

func (e *TransitionError) Unwrap() error { return ErrInvalidTransition }

// QuotaError 携带配额申请与剩余信息，便于解释超额原因。
type QuotaError struct {
	Requested Resources
	Available Resources
	Resource  string // cpuMillis / memoryMiB / diskMiB
}

func (e *QuotaError) Error() string {
	return fmt.Sprintf("quota exceeded on %s: requested exceeds available", e.Resource)
}

func (e *QuotaError) Unwrap() error { return ErrQuotaExceeded }
