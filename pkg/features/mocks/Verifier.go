// Code generated by mockery v2.22.1. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// Verifier is an autogenerated mock type for the Verifier type
type Verifier struct {
	mock.Mock
}

// Verify provides a mock function with given fields: name
func (_m *Verifier) Verify(name string) (bool, error) {
	ret := _m.Called(name)

	var r0 bool
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (bool, error)); ok {
		return rf(name)
	}
	if rf, ok := ret.Get(0).(func(string) bool); ok {
		r0 = rf(name)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(name)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewVerifier interface {
	mock.TestingT
	Cleanup(func())
}

// NewVerifier creates a new instance of Verifier. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewVerifier(t mockConstructorTestingTNewVerifier) *Verifier {
	mock := &Verifier{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
