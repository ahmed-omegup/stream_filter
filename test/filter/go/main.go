package main

import (
	"bufio"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"
)

func main() {
	// Environment variables with defaults
	routerHost := getEnv("ROUTER_HOST", "127.0.0.1")
	routerPort := getEnvInt("ROUTER_PORT", 8000)
	numCustomers := getEnvInt("NUM_CUSTOMERS", 100000)
	maxDate := getEnvInt("MAX_DATE", 100000)
	maxSpan := getEnvInt("MAX_SPAN", 10)

	// Create interval tree
	tree := NewTreeNode(1, maxDate)

	// Generate customers
	customers := generateCustomers(numCustomers, maxDate, maxSpan)
	for _, customer := range customers {
		tree.Insert(customer)
	}

	// Connect to router
	routerAddr := fmt.Sprintf("%s:%d", routerHost, routerPort)
	routerConn, err := net.Dial("tcp", routerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to router: %v", err)
	}
	defer routerConn.Close()

	// Start TCP server
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	fmt.Println("Listening on 0.0.0.0:8080")

	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		go handleConnection(conn, tree, routerConn)
	}
}

func handleConnection(conn net.Conn, tree *TreeNode, routerConn net.Conn) {
	defer conn.Close()

	start := time.Now()
	messages := 0
	buffer := list.New()

	reader := bufio.NewReader(conn)
	
	for {
		// Read data
		chunk := make([]byte, 10240)
		n, err := reader.Read(chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("Read error: %v", err)
			break
		}

		// Add bytes to buffer list
		for i := 0; i < n; i++ {
			buffer.PushBack(chunk[i])
		}
		
		newMessages := processBuffer(buffer, tree, routerConn)
		messages += newMessages
	}

	duration := time.Since(start).Milliseconds()
	fmt.Printf("Duration (ms): %d\n", duration)
	fmt.Printf("Handled messages: %d\n", messages)
}

func processBuffer(buffer *list.List, tree *TreeNode, routerConn net.Conn) int {
	messages := 0
	
	for buffer.Len() >= 4 {
		// Read message length from first 4 bytes
		msgLenBytes := make([]byte, 4)
		i := 0
		for e := buffer.Front(); e != nil && i < 4; e = e.Next() {
			msgLenBytes[i] = e.Value.(byte)
			i++
		}
		
		msgLen := binary.BigEndian.Uint32(msgLenBytes)
		
		// Check if we have the complete message
		if buffer.Len() < int(4+msgLen) {
			break
		}
		
		// Extract message bytes
		msgData := make([]byte, msgLen)
		
		// Remove length bytes
		for i := 0; i < 4; i++ {
			buffer.Remove(buffer.Front())
		}
		
		// Extract message data
		for i := 0; i < int(msgLen); i++ {
			msgData[i] = buffer.Front().Value.(byte)
			buffer.Remove(buffer.Front())
		}
		
		// Process message
		var event Event
		if err := json.Unmarshal(msgData, &event); err != nil {
			log.Printf("Failed to decode JSON: %v", err)
			continue
		}
		
		// Dispatch to interval tree
		tree.Dispatch(event.Date, func(customerID int) {
			// Send to router
			data := make([]byte, 4)
			binary.BigEndian.PutUint32(data, uint32(customerID))
			routerConn.Write(data)
		})
		
		messages++
	}
	
	return messages
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func generateCustomers(n, maxDate, maxSpan int) []Customer {
	rand.Seed(42)
	customers := make([]Customer, n)
	
	for i := 0; i < n; i++ {
		start := rand.Intn(maxDate-maxSpan) + 1
		span := rand.Intn(maxSpan) + 1
		customers[i] = Customer{
			ID:    i + 1,
			Start: start,
			End:   start + span,
		}
	}
	
	return customers
}

type Event struct {
	ID   int    `json:"id"`
	Date int    `json:"date"`
	Type string `json:"type"`
}
