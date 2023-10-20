package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/ozeemandias/auth/pkg/user_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
)

const grpcPort = 50051

type server struct {
	user_v1.UnimplementedUserV1Server
}

func (s *server) Create(_ context.Context, req *user_v1.CreateRequest) (*user_v1.CreateResponse, error) {
	log.Printf("%v", req)

	return &user_v1.CreateResponse{}, nil
}

func (s *server) Get(_ context.Context, req *user_v1.GetRequest) (*user_v1.GetResponse, error) {
	log.Printf("%v", req)

	return &user_v1.GetResponse{}, nil
}

func (s *server) Update(_ context.Context, req *user_v1.UpdateRequest) (*emptypb.Empty, error) {
	log.Printf("%v", req)

	return &emptypb.Empty{}, nil
}

func (s *server) Delete(_ context.Context, req *user_v1.DeleteRequest) (*emptypb.Empty, error) {
	log.Printf("%v", req)

	return &emptypb.Empty{}, nil
}

func main() {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	reflection.Register(s)
	user_v1.RegisterUserV1Server(s, &server{})

	log.Printf("server listening at %v", ln.Addr())

	if err = s.Serve(ln); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
