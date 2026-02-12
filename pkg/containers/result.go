package containers

type Result[T any] struct {
	Value T
	Err   error
}

func (r *Result[T]) IsOk() bool {
	return r.Err == nil
}

func (r *Result[T]) IsErr() bool {
	return r.Err != nil
}

func (r *Result[T]) Unwrap() T {
	if r.IsErr() {
		panic("called Unwrap on an Err result")
	}
	return r.Value
}

func (r *Result[T]) Error() error {
	if r.IsOk() {
		panic("called UnwrapErr on an Ok result")
	}
	return r.Err
}

func (r *Result[T]) UnwrapOr(defaultValue T) T {
	if r.IsErr() {
		return defaultValue
	}
	return r.Value
}

func (r *Result[T]) UnwrapOrElse(f func() T) T {
	if r.IsErr() {
		return f()
	}
	return r.Value
}

func Ok[T any](value T) Result[T] {
	return Result[T]{Value: value}
}

func Err[T any](err error) Result[T] {
	return Result[T]{Err: err}
}

func NewResult[T any](err error, value T) Result[T] {
	if err != nil {
		return Err[T](err)
	}
	return Ok(value)
}
