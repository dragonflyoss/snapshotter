package dragonfly

import (
	"testing"

	"google.golang.org/grpc"
)

func TestWithContentReusesConnection(t *testing.T) {
	conn := &grpc.ClientConn{}
	base := &client{
		conn:               conn,
		provider:           ContentProvider{Provider: "oss", Bucket: "old"},
		snapshotterRootDir: "/snapshotter",
	}
	provider := ContentProvider{Provider: "oss", Bucket: "new"}

	view, ok := base.WithContent(provider).(*client)
	if !ok {
		t.Fatal("WithContent() did not return a Dragonfly client")
	}
	if view == base {
		t.Fatal("WithContent() mutated and returned the base client")
	}
	if view.conn != conn {
		t.Fatal("WithContent() did not reuse the gRPC connection")
	}
	if view.provider != provider {
		t.Fatalf("provider = %+v, want %+v", view.provider, provider)
	}
	if base.provider.Bucket != "old" {
		t.Fatalf("base provider was mutated: %+v", base.provider)
	}
}
