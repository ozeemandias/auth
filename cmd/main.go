package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"net"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ozeemandias/auth/internal/config"
	"github.com/ozeemandias/auth/pkg/user_v1"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var envPath string

func init() {
	flag.StringVar(&envPath, "env", ".env", "path to env file")
}

type server struct {
	user_v1.UnimplementedUserV1Server
	dbpool *pgxpool.Pool
}

type userRole user_v1.Role

func (dest *userRole) Scan(v interface{}) error {
	ns := sql.NullString{}
	if err := ns.Scan(v); err != nil {
		return err
	}

	if !ns.Valid {
		*dest = userRole(user_v1.Role_UNSPECIFIED)
		return nil
	}

	if val, exists := user_v1.Role_value[strings.ToUpper(ns.String)]; exists {
		*dest = userRole(val)
		return nil
	}

	*dest = userRole(user_v1.Role_UNSPECIFIED)

	return nil
}

func hashAndSalt(pwd []byte) (string, error) {
	hash, err := bcrypt.GenerateFromPassword(pwd, bcrypt.MinCost)
	if err != nil {
		return "", err
	}

	return string(hash), nil
}

func (s *server) Create(ctx context.Context, req *user_v1.CreateRequest) (*user_v1.CreateResponse, error) {
	password, err := hashAndSalt([]byte(req.Password))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to hash password: %v", err)
	}

	query, args, err := sq.Insert("users").
		Columns("name", "email", "password", "role").
		Values(strings.TrimSpace(req.Name), strings.ToLower(strings.TrimSpace(req.Email)), password, strings.ToLower(req.Role.String())).
		PlaceholderFormat(sq.Dollar).
		Suffix("RETURNING id").
		ToSql()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build query: %v", err)
	}

	var userID int64
	err = s.dbpool.QueryRow(ctx, query, args...).Scan(&userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to insert user: %v", err)
	}

	log.Printf("inserted user with id: %d", userID)

	return &user_v1.CreateResponse{
		Id: userID,
	}, nil
}

func (s *server) Get(ctx context.Context, req *user_v1.GetRequest) (*user_v1.GetResponse, error) {
	query, args, err := sq.Select("id", "name", "email", "role", "created_at", "updated_at").
		From("users").
		Where(sq.Eq{"id": req.GetId()}).
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build query: %v", err)
	}

	var id int64
	var name, email string
	var role userRole
	var createdAt time.Time
	var updatedAt time.Time
	err = s.dbpool.QueryRow(ctx, query, args...).Scan(&id, &name, &email, &role, &createdAt, &updatedAt)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to select user: %v", err)
	}

	log.Printf("id: %d, name: %s, email: %s, role: %d, created_at: %v, updated_at: %v\n", id, name, email, role, createdAt, updatedAt)

	return &user_v1.GetResponse{
		Id:        id,
		Name:      name,
		Email:     email,
		Role:      user_v1.Role(role),
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(updatedAt),
	}, nil
}

func (s *server) Update(ctx context.Context, req *user_v1.UpdateRequest) (*emptypb.Empty, error) {
	updateBuilder := sq.Update("users").
		Where(sq.Eq{"id": req.GetId()}).
		PlaceholderFormat(sq.Dollar)

	if req.Email != nil {
		if value := strings.TrimSpace(req.Email.Value); value != "" {
			updateBuilder = updateBuilder.Set("email", strings.ToLower(value))
		}
	}

	if req.Name != nil {
		if value := strings.TrimSpace(req.Name.Value); value != "" {
			updateBuilder = updateBuilder.Set("name", value)
		}
	}

	if req.Role != user_v1.Role_UNSPECIFIED {
		updateBuilder = updateBuilder.Set("role", strings.ToLower(req.Role.String()))
	}

	updateBuilder = updateBuilder.Set("updated_at", sq.Expr("NOW()"))

	query, args, err := updateBuilder.ToSql()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build query: %v", err)
	}

	ct, err := s.dbpool.Exec(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update user: %v", err)
	}

	log.Printf("updated user count: %d", ct.RowsAffected())

	return &emptypb.Empty{}, nil
}

func (s *server) Delete(ctx context.Context, req *user_v1.DeleteRequest) (*emptypb.Empty, error) {
	query, args, err := sq.Delete("users").
		Where(sq.Eq{"id": req.GetId()}).
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build query: %v", err)
	}

	ct, err := s.dbpool.Exec(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete user: %v", err)
	}

	log.Printf("deleted user count: %d", ct.RowsAffected())

	return &emptypb.Empty{}, nil
}

func main() {
	ctx := context.Background()

	// Считываем переменные окружения
	err := config.Load(envPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	grpcConfig, err := config.NewGRPCConfig()
	if err != nil {
		log.Fatalf("failed to get grpc config: %v", err)
	}

	pgConfig, err := config.NewPGConfig()
	if err != nil {
		log.Fatalf("failed to get pg config: %v", err)
	}

	ln, err := net.Listen("tcp", grpcConfig.Address())
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	dbpool, err := pgxpool.New(ctx, pgConfig.DSN())
	if err != nil {
		log.Fatalf("unable to create connection pool: %v\n", err)
	}
	defer dbpool.Close()

	s := grpc.NewServer()
	reflection.Register(s)
	user_v1.RegisterUserV1Server(s, &server{dbpool: dbpool})

	log.Printf("server listening at %v", ln.Addr())

	if err = s.Serve(ln); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
