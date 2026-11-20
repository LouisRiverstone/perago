package perago

import (
	"errors"
	"fmt"
)

// Code is a stable, machine-readable error identifier.
type Code string

const (
	InvalidConfig          Code = "INVALID_CONFIG"
	ConnectionNotFound     Code = "CONNECTION_NOT_FOUND"
	JobNotRegistered       Code = "JOB_NOT_REGISTERED"
	PayloadInvalid         Code = "PAYLOAD_INVALID"
	PayloadDecryptFailed   Code = "PAYLOAD_DECRYPT_FAILED"
	ClosureNotSerializable Code = "CLOSURE_NOT_SERIALIZABLE"
	PushFailed             Code = "PUSH_FAILED"
	PopFailed              Code = "POP_FAILED"
	DriverFailed           Code = "DRIVER_FAILED"
	JobFailedManually      Code = "JOB_FAILED_MANUALLY"
	JobTimeout             Code = "JOB_TIMEOUT"
	MaxAttemptsExceeded    Code = "MAX_ATTEMPTS_EXCEEDED"
	MaxExceptionsExceeded  Code = "MAX_EXCEPTIONS_EXCEEDED"
	BatchNotFound          Code = "BATCH_NOT_FOUND"
	BatchFailed            Code = "BATCH_FAILED"
	FailedJobNotFound      Code = "FAILED_JOB_NOT_FOUND"
	StoreFailed            Code = "STORE_FAILED"
	CacheFailed            Code = "CACHE_FAILED"
	FailoverExhausted      Code = "FAILOVER_EXHAUSTED"
	ModelNotFound          Code = "MODEL_NOT_FOUND"
	NotInJob               Code = "NOT_IN_JOB"
)

// Error is the single error type returned across perago boundaries.
// Match with errors.Is(err, perago.ErrMaxAttemptsExceeded) or perago.CodeOf.
type Error struct {
	Code    Code
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("perago: %s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("perago: %s: %s: %v", e.Code, e.Message, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Is matches on Code so sentinels work with errors.Is.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// Sentinels for errors.Is.
var (
	ErrConnectionNotFound     = &Error{Code: ConnectionNotFound}
	ErrJobNotRegistered       = &Error{Code: JobNotRegistered}
	ErrClosureNotSerializable = &Error{Code: ClosureNotSerializable}
	ErrJobTimeout             = &Error{Code: JobTimeout}
	ErrMaxAttemptsExceeded    = &Error{Code: MaxAttemptsExceeded}
	ErrMaxExceptionsExceeded  = &Error{Code: MaxExceptionsExceeded}
	ErrBatchNotFound          = &Error{Code: BatchNotFound}
	ErrFailedJobNotFound      = &Error{Code: FailedJobNotFound}
	ErrModelNotFound          = &Error{Code: ModelNotFound}
	ErrNotInJob               = &Error{Code: NotInJob}
)

// Wrap builds an *Error around a cause.
func Wrap(code Code, cause error, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Err: cause}
}

// Newf builds an *Error without a cause.
func Newf(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// CodeOf extracts the perago Code from err, or "" when err is not a perago error.
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// IsCode reports whether err is a perago error with code.
func IsCode(err error, code Code) bool { return CodeOf(err) == code }
