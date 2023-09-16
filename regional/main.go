package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "SD/tarea1/protocol"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

type server struct {
	pb.UnimplementedKeyServiceServer
}

func (s *server) SendKeys(ctx context.Context, msg *pb.CentralToRegionalAvailableKeysRequest) (*pb.CentralToRegionalAvailableKeysResponse, error) {

	fmt.Println("[Servidor Regional] Central notifica un total de ", msg.Ammount, " keys disponibles.")

	return &pb.CentralToRegionalAvailableKeysResponse{Code: 200}, nil
}

func StartServer() {
	lis, err := net.Listen("tcp", ":50000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	serv := grpc.NewServer()
	defer serv.GracefulStop()
	pb.RegisterKeyServiceServer(serv, &server{})

	if err := serv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func AsyncQuequeSendMessage(message string) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"Central",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := message
	err = ch.PublishWithContext(ctx,
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s\n", body)
}

func main() {

	StartServer()
	AsyncQuequeSendMessage("1:500")
}
