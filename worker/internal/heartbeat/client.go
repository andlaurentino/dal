// Package heartbeat wraps the gRPC client calls a worker makes to the
// control plane's WorkerManager service: registering on boot and reporting
// periodic health.
package heartbeat

import (
	"context"

	controlplanev1 "github.com/andersonlaurentino/dal-core/proto/gen/controlplane/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn *grpc.ClientConn
	rpc  controlplanev1.WorkerManagerClient
}

func Dial(controlplaneAddr string) (*Client, error) {
	conn, err := grpc.NewClient(controlplaneAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, rpc: controlplanev1.NewWorkerManagerClient(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Register(ctx context.Context, syncName string, pid int64) error {
	_, err := c.rpc.RegisterWorker(ctx, &controlplanev1.RegisterWorkerRequest{SyncName: syncName, Pid: pid})
	return err
}

func (c *Client) Heartbeat(ctx context.Context, syncName, phase string, consumerLag int64, watermark string, timestampUnix int64) error {
	_, err := c.rpc.Heartbeat(ctx, &controlplanev1.HeartbeatRequest{
		SyncName:      syncName,
		Phase:         phase,
		ConsumerLag:   consumerLag,
		Watermark:     watermark,
		TimestampUnix: timestampUnix,
	})
	return err
}

func (c *Client) ReportError(ctx context.Context, syncName, message string, fatal bool) error {
	_, err := c.rpc.ReportError(ctx, &controlplanev1.ReportErrorRequest{SyncName: syncName, Message: message, Fatal: fatal})
	return err
}
