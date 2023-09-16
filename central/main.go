package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"strings"

	pb "SD/tarea1/protocol"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

// Set to virtual machine ips
var G_SERVERS_IPS = [4]string{
	"127.0.0.1",
	"127.0.0.1",
	"127.0.0.1",
	"127.0.0.1",
}

var G_SERVERS_PORT = [4]string{
	":50000",
	":50001",
	":50002",
	":50003",
}

var G_KEYS = 0

var recepcion = [4]int{1, 1, 1, 1}
var c_recept = 0
var aux_iteraciones = [2]int{0, 0}
var anteriores = [4]int{0, 0, 0, 0}
var operatorio = 0
var minimo = 0
var maximo = 0

func AsyncQueque() {
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

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	for d := range msgs {
		var data = strings.Split(string(d.Body), ":")
		var server, _ = strconv.Atoi(data[0])
		var value, _ = strconv.Atoi(data[1])

		algoritmo_asincrono(server, value)
		fmt.Println("[Central] Mensaje del servidor ", server, " leido")
	}

	<-forever
}

func sendKeys(server int, keys int) {

	connS, err1 := grpc.Dial(G_SERVERS_IPS[server]+G_SERVERS_PORT[server], grpc.WithInsecure())
	if err1 != nil {
		panic("No se pudo conectar al servidor regional" + err1.Error())
	}
	defer connS.Close()

	serviceCliente := pb.NewKeyServiceClient(connS)

	_, err2 := serviceCliente.SendKeys(context.Background(),
		&pb.CentralToRegionalAvailableKeysRequest{
			Ammount: int32(keys),
		})
	if err2 != nil {
		panic("No se puede crear el mensaje " + err2.Error())
	}

}

func lectura() (int, int, int) {
	datosComoBytes, err := ioutil.ReadFile("parametros_de_inicio.txt")
	if err != nil {
		log.Fatal(err)
	}

	datosComoString := string(datosComoBytes)
	lineas := strings.Split(datosComoString, "\n")
	partes := strings.Split(lineas[0], "-")
	aleat := lineas[1]
	min := partes[0]
	max := partes[1]
	aleat = strings.TrimRight(aleat, "\n")

	e_aleat, err := strconv.Atoi(aleat)
	if err != nil {
		log.Fatal(err)
	}
	e_min, err := strconv.Atoi(min)
	if err != nil {
		log.Fatal(err)
	}
	e_max, err := strconv.Atoi(max)
	if err != nil {
		log.Fatal(err)
	}

	return e_aleat, e_min, e_max
}

func aleatorio(menor, mayor int) int {
	return rand.Intn(mayor-menor) + menor
}

func algoritmo_asincrono(n_server, m_server int) {
	if operatorio > 0 {
		if operatorio <= m_server {
			m_server = m_server - operatorio
			fmt.Println("[Central] Se inscribieron ", operatorio, " cupos del servidor ", (n_server + 1))
			operatorio = 0
			sendKeys(n_server, m_server)
			anteriores[n_server] = m_server
		} else {
			operatorio = operatorio - m_server
			fmt.Println("[Central] Se inscribieron : ", m_server, "del servidor", (n_server + 1))
			sendKeys(n_server, m_server)
			if m_server == 0 {
				anteriores[n_server] = -1
			} else {
				anteriores[n_server] = m_server
			}
		}
	} else {
		sendKeys(n_server, m_server)
	}
}

func asincrono_simulado(n_server, m_server int) {
	if (recepcion[0] == 1) && (recepcion[1] == 1) && (recepcion[2] == 1) && (recepcion[3] == 1) {
		if (anteriores[0] == -1) && (anteriores[1] == -1) && (anteriores[2] == -1) && (anteriores[3] == -1) {
			sendKeys(0, -1)
			sendKeys(1, -1)
			sendKeys(2, -1)
			sendKeys(3, -1)
		} else {
			c_recept = 0
			recepcion[0] = 0
			recepcion[1] = 0
			recepcion[2] = 0
			recepcion[3] = 0
			fmt.Println("---------------------")
			fmt.Println("Generacion : ", aux_iteraciones[1], " de ", aux_iteraciones[0])
			operatorio = aleatorio(minimo, maximo)
			fmt.Println("-Cupos : ", operatorio)
			fmt.Println("---------------------")
			sendKeys(0, operatorio)
			sendKeys(1, operatorio)
			sendKeys(2, operatorio)
			sendKeys(3, operatorio)
			aux_iteraciones[1] = aux_iteraciones[1] + 1
			algoritmo_asincrono(n_server, m_server)
			c_recept = c_recept + 1
		}
	} else {
		algoritmo_asincrono(n_server, m_server)
		if (aux_iteraciones[1] == aux_iteraciones[0]) && (c_recept == 3) {
			sendKeys(0, -1)
			sendKeys(1, -1)
			sendKeys(2, -1)
			sendKeys(3, -1)
		}
		c_recept = c_recept + 1
	}
}

func main() {

	// Get data
	aux_iteraciones[0], minimo, maximo = lectura()

	// FOR PRINT FORMAT ONLY
	var topString = ""
	if aux_iteraciones[0] == -1 {
		topString = "Infinito"
	} else {
		topString = strconv.Itoa(aux_iteraciones[0])
	}

	// Generate keys in every iteration
	for i := 0; i < aux_iteraciones[0]; i++ {
		G_KEYS += aleatorio(minimo, maximo)
		fmt.Println("[Central] Generacion ", i+1, "/", topString, ".")

		// Notify servers with current available keys
		for i := 0; i < 4; i++ {
			sendKeys(i, G_KEYS)
		}
	}
}
