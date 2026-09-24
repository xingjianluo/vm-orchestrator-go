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
