// Package grpcclient wraps the broker's gRPC call to the control plane's
// QueryPlanner service, used to resolve which store/connection/table should
// answer a query against a named Projection.
package grpcclient

import (
	"context"

	controlplanev1 "github.com/andersonlaurentino/dal-core/proto/gen/controlplane/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn *grpc.ClientConn
	rpc  controlplanev1.QueryPlannerClient
}

func Dial(controlplaneAddr string) (*Client, error) {
	conn, err := grpc.NewClient(controlplaneAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, rpc: controlplanev1.NewQueryPlannerClient(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) ResolvePlan(ctx context.Context, projectionName string) (*controlplanev1.ResolvePlanResponse, error) {
	return c.rpc.ResolvePlan(ctx, &controlplanev1.ResolvePlanRequest{ProjectionName: projectionName})
}
