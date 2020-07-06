package grpc_haclient

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
)

type clientConnManager struct {
	mutex             sync.RWMutex
	connSet           map[string]*grpc.ClientConn // endpoint ==> *grpc.ClientConn
	endpoints         []string                    // endpoints
	lastRoundRobinIdx int32                       // 记录上次roundrobin的位置
}

func newClientConnManager() *clientConnManager {
	return &clientConnManager{
		connSet:           make(map[string]*grpc.ClientConn),
		endpoints:         make([]string, 0),
		lastRoundRobinIdx: -1,
	}
}

func (m *clientConnManager) CloseAll() {
	m.mutex.Lock()
	for _, cc := range m.connSet {
		if cc != nil {
			cc.Close()
		}
	}
	m.mutex.Unlock()
}

func (m *clientConnManager) ResetConn(endpoint string, cc *grpc.ClientConn) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if oldConn, _ := m.connSet[endpoint]; oldConn != nil {
		oldConn.Close()
	}
	m.connSet[endpoint] = cc

	// 将所有endpoint整合到一起，按照名字排序
	var idx = 0
	for ; idx < len(m.endpoints); idx++ {
		if m.endpoints[idx] == endpoint {
			continue
		}
	}
	if idx >= len(m.endpoints) {
		m.endpoints = append(m.endpoints, endpoint)
	}
	sort.Strings(m.endpoints)
}

func (m *clientConnManager) GetConn(endpoint string) *grpc.ClientConn {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.connSet[endpoint]
}

func (m *clientConnManager) FirstAvailableConn() (*grpc.ClientConn, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for _, endpoint := range m.endpoints {
		if cc, _ := m.connSet[endpoint]; cc != nil {
			return cc, nil
		}
	}
	return nil, errors.New("no available connection found")
}

func (m *clientConnManager) RoundRobinConn() (*grpc.ClientConn, error) {
	var firstEndpoint string
	for {
		cc, endpoint := m.roundRobin()
		if endpoint == "" {
			return nil, errors.New("no available connection found")
		}
		if cc != nil {
			return cc, nil
		}
		if firstEndpoint == "" {
			firstEndpoint = endpoint
			continue
		}
		if firstEndpoint == endpoint {
			return nil, errors.New("no available connection found")
		}
	}
}

func (m *clientConnManager) roundRobin() (cc *grpc.ClientConn, endpoint string) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if len(m.endpoints) > 0 {
		endpoint = m.endpoints[atomic.AddInt32(&m.lastRoundRobinIdx, 1)%int32(len(m.endpoints))]
		return m.connSet[endpoint], endpoint
	}
	return nil, ""
}
