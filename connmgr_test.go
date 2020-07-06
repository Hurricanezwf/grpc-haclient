// +build unittest

package gprc_haclient

import (
	"testing"

	"google.golang.org/grpc"
)

func TestClientConnManager(t *testing.T) {
	mgr := newClientConnManager()
	mgr.ResetConn("0.0.0.1", nil)
	mgr.ResetConn("0.0.0.2", nil)

	if _, err := mgr.FirstAvailableConn(); err == nil {
		t.Fatalf("it must get error returned")
	}
	if _, err := mgr.RoundRobinConn(); err == nil {
		t.Fatalf("it must get error returned")
	}

	//
	mgr2 := newClientConnManager()
	mgr2.ResetConn("0.0.0.1", nil)
	mgr2.ResetConn("0.0.0.2", nil)
	mgr2.ResetConn("0.0.0.3", &grpc.ClientConn{})
	mgr2.ResetConn("0.0.0.4", &grpc.ClientConn{})
	mgr2.ResetConn("0.0.0.5", &grpc.ClientConn{})
	if _, err := mgr2.FirstAvailableConn(); err != nil {
		t.Fatalf("it must not get error returned")
	}
	for i := 0; i < 3; i++ {
		if _, err := mgr2.RoundRobinConn(); err != nil {
			t.Fatalf("it must not get error returned")
		}
	}
}
