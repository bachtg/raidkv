package client

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	api "github.com/bachtg/raidkv/api/raidkv/v1"
)

var ErrNotFound = errors.New("raidkv: key not found")

type Client struct {
	conn *grpc.ClientConn
	kv   api.KVServiceClient
}

func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	return New(conn), nil
}

func New(conn *grpc.ClientConn) *Client {
	return &Client{conn: conn, kv: api.NewKVServiceClient(conn)}
}

func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	resp, err := c.kv.Get(ctx, &api.GetRequest{Key: key})
	if status.Code(err) == codes.NotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return resp.GetValue(), nil
}

func (c *Client) Put(ctx context.Context, key, value []byte) error {
	_, err := c.kv.Put(ctx, &api.PutRequest{Key: key, Value: value})
	return err
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	_, err := c.kv.Delete(ctx, &api.DeleteRequest{Key: key})
	return err
}

func (c *Client) Close() error {
	return c.conn.Close()
}
