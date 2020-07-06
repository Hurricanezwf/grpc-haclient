package gprc_haclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// ReadinessProbeRPC 是对远程服务端获取readiness状态的rpc调用抽象
type ReadinessProbeRPC func(cc *grpc.ClientConn) error

// HAClient 是对grpc连接的高可用的抽象
type HAClient interface {
	// Close 释放资源，必须被调用
	Close()

	// RoundRobin 轮询使用配置的连接
	RoundRobin() (*grpc.ClientConn, error)

	// FirstAvailable 获取当前第一个可用的连接
	FirstAvailable() (*grpc.ClientConn, error)
}

func New(endpoints []string, readinessProbeRPC ReadinessProbeRPC, opts ...haClientOption) (HAClient, error) {
	return newHAClient(endpoints, readinessProbeRPC, opts...)
}

// haclient 实现了 HAClient 的抽象接口
type haclient struct {
	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc

	// readinessprobeRPC 上层传下来的用于检测服务端readiness状态的rpc调用
	readinessProbeRPC ReadinessProbeRPC

	// dialTimeout 与远程建立TCP连接的超时时间
	dialTimeout time.Duration

	keepaliveWaitGroup   sync.WaitGroup
	endpoints            []string
	availableConnManager *clientConnManager
}

func newHAClient(endpoints []string, readinessProbeRPC ReadinessProbeRPC, opts ...haClientOption) (*haclient, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("no endpoints found")
	}
	if readinessProbeRPC == nil {
		return nil, errors.New("readinessProbeRPC cannot be nil")
	}

	c := &haclient{}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.endpoints = endpoints
	c.readinessProbeRPC = readinessProbeRPC
	c.availableConnManager = newClientConnManager()
	c.dialTimeout = 5 * time.Second
	for _, opt := range opts {
		opt(c)
	}

	go c.keepalive()

	for i := 0; i < 30; i++ {
		time.Sleep(2 * time.Second)
		if _, err := c.availableConnManager.FirstAvailableConn(); err == nil {
			return c, nil
		}
	}
	c.Close()
	return nil, fmt.Errorf("no available endpoints %s", strings.Join(endpoints, ","))
}

// keepalive 与 endpoints 保持联系
func (c *haclient) keepalive() {
	c.keepaliveWaitGroup.Add(len(c.endpoints))

	for _, endpoint := range c.endpoints {
		// do health check
		go func(endpoint string) {
			defer c.keepaliveWaitGroup.Done()

			var err error
			var ticker = time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-c.ctx.Done():
					return
				case <-ticker.C:
					cc := c.availableConnManager.GetConn(endpoint)
					// 连接存在，进行readiness probe
					if cc != nil {
						if err = c.readinessProbeRPC(cc); err != nil {
							fmt.Printf("%s endpoint `%s` is unhealthy now", c.logPrefix(), endpoint)
							c.availableConnManager.ResetConn(endpoint, nil)
						}
						continue
					}
					// 连接不存在，新建连接
					if cc, err = c.dial(endpoint); err != nil {
						fmt.Printf("%s endpoint `%s` is bad, dial failed, %v", c.logPrefix(), endpoint, err)
						continue
					}
					if err = c.readinessProbeRPC(cc); err != nil {
						fmt.Printf("%s endpoint `%s` is unhealthy now", c.logPrefix(), endpoint)
						cc.Close()
						continue
					}
					c.availableConnManager.ResetConn(endpoint, cc)
				}
			}
		}(endpoint)
	}
}

func (c *haclient) dial(endpoint string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.dialTimeout)
	defer cancel()
	return grpc.DialContext(ctx, endpoint, grpc.WithInsecure())
}

func (c *haclient) Close() {
	c.once.Do(func() {
		c.availableConnManager.CloseAll()
	})
}

func (c *haclient) FirstAvailable() (*grpc.ClientConn, error) {
	return c.availableConnManager.FirstAvailableConn()
}

func (c *haclient) RoundRobin() (*grpc.ClientConn, error) {
	return c.availableConnManager.RoundRobinConn()
}

func (c *haclient) logPrefix() string {
	return "grpc-haclient:"
}

// haClientOption 是对 haclient 的可选项的抽象
type haClientOption func(c *haclient)

// WithDialTimeout 设置grpc.Dial的超时时间, 默认为 5s
func WithDialTimeout(timeout time.Duration) haClientOption {
	return func(c *haclient) {
		c.dialTimeout = timeout
	}
}
