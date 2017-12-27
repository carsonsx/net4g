package net4g

type Intercepter interface {
	Intercept(data []byte) []byte
}