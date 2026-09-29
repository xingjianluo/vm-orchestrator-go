package domain

import (
	"errors"
	"fmt"
)

// ErrorKind 是可程序化区分的错误类别，调用方可用 errors.Is / IsKind 精确分支。
type ErrorKind string

const (
	KindNotFound             ErrorKind = "not_found"
	KindAlreadyExists        ErrorKind = "already_exists"
	KindIllegalTransition    ErrorKind = "illegal_transition"
	KindInsufficientResource ErrorKind = "insufficient_resource"
	KindBackendFailure       ErrorKind = "backend_failure"
	KindInconsistentState    ErrorKind = "inconsistent_state"
	KindInvalidArgument      ErrorKind = "invalid_argument"
	// KindManualReviewPending 实例处于待人工处理冻结态：常规变更被拒，
	// 必须先经人工显式收口（resolve adopt / resolve discard）。
	KindManualReviewPending ErrorKind = "manual_review_pending"
	// KindHostOvercommitted 宿主当前占用总额已经超过配置容量（例如对账重建出
	// 超大残留），任何会新增占用的准入一律拒绝；但查询、收口等不新增占用的
	// 命令不受影响。它是"准入策略"拒绝，与内部漂移的 inconsistent_state 明确区分。
	KindHostOvercommitted ErrorKind = "host_overcommitted"
)

// OpError 携带可区分的错误类别与上下文。
type OpError struct {
	Kind       ErrorKind
	Op         string
	InstanceID string
	// Resource 在配额类错误中标识首个不足的资源维度。
	Resource  string
	Reason    string
	Requested int64
	Available int64
	Err       error
}

func (e *OpError) Error() string {
	s := string(e.Kind)
	if e.Op != "" {
		s += " [" + e.Op + "]"
	}
	if e.InstanceID != "" {
		s += " instance=" + e.InstanceID
	}
	if e.Resource != "" {
		s += fmt.Sprintf(" resource=%s(requested=%d,available=%d)", e.Resource, e.Requested, e.Available)
	}
	if e.Reason != "" {
		s += ": " + e.Reason
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

func (e *OpError) Unwrap() error { return e.Err }

// IsKind 报告 err 链上是否存在指定类别的 OpError。
func IsKind(err error, kind ErrorKind) bool {
	var oe *OpError
	if errors.As(err, &oe) {
		return oe.Kind == kind
	}
	return false
}

func NewNotFoundError(op, id string) error {
	return &OpError{Kind: KindNotFound, Op: op, InstanceID: id, Reason: "instance not found"}
}

func NewAlreadyExistsError(op, id string) error {
	return &OpError{Kind: KindAlreadyExists, Op: op, InstanceID: id, Reason: "instance already exists; duplicate create rejected"}
}

func NewIllegalTransitionError(op, id string, from, to State, reason string) error {
	return &OpError{
		Kind:       KindIllegalTransition,
		Op:         op,
		InstanceID: id,
		Reason:     reason,
	}
}

func NewInsufficientResourceError(op, id, resource string, requested, available int64) error {
	return &OpError{
		Kind:       KindInsufficientResource,
		Op:         op,
		InstanceID: id,
		Resource:   resource,
		Requested:  requested,
		Available:  available,
		Reason:     "host quota exhausted; admission rejected before any side effect",
	}
}

func NewBackendError(op, id string, err error) error {
	return &OpError{Kind: KindBackendFailure, Op: op, InstanceID: id, Err: err}
}

func NewInvalidArgumentError(op, reason string) error {
	return &OpError{Kind: KindInvalidArgument, Op: op, Reason: reason}
}

func NewInconsistentStateError(op, id, reason string) error {
	return &OpError{Kind: KindInconsistentState, Op: op, InstanceID: id, Reason: reason}
}

// NewManualReviewPendingError 构造"待人工处理"拒绝错误，携带判定原因便于调用方区分。
func NewManualReviewPendingError(op, id string, rv *Review) error {
	reason := "instance is held for manual review; use resolve adopt/resolve discard"
	if rv != nil {
		reason += " (reason=" + string(rv.Reason) + ": " + rv.Detail + ")"
	}
	return &OpError{Kind: KindManualReviewPending, Op: op, InstanceID: id, Reason: reason}
}

// NewHostOvercommittedError 构造"宿主已超卖"准入拒绝错误：
// 重建占用已超容量时，任何会新增占用的请求都以此类别拒绝（区别于内部不一致）。
func NewHostOvercommittedError(op, id, resource string, requested, available int64) error {
	return &OpError{
		Kind:       KindHostOvercommitted,
		Op:         op,
		InstanceID: id,
		Resource:   resource,
		Requested:  requested,
		Available:  available,
		Reason:     "host already overcommitted beyond configured capacity; new admission rejected (queries and manual resolves stay available)",
	}
}
