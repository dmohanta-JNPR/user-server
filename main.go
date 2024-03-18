package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

var testUsers []User
var producer sarama.SyncProducer
var topic string  = "userops"
var message string

type User struct {
	Id      string `json:"id"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

func getRoot(w http.ResponseWriter, r *http.Request) {
	fmt.Println("got \"\\\" request")

	message = "root"
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalln("Failed to send message:", err)
	}

	io.WriteString(w, "<h1> User Server </h1>")
}

func getAllUser(w http.ResponseWriter, r *http.Request) {

	// fmt.Println("r = ", r)
	// fmt.Println("end")

	// users.add(testUsers)

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")

	message = "getalluser"
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalln("Failed to send message:", err)
	}

	// log.Println(testUsers)

	// jsonResp, err := json.MarshalIndent(testUsers, " ", "")

	// log.Println(string(jsonResp))

	// if err != nil {
	// 	log.Fatalf("Error happened in JSON marshal. Err: %s", err)
	// }

	// w.Write(jsonResp)
	// io.WriteString(w,"got all user")
}

func getUserById(w http.ResponseWriter, r *http.Request) {
	userId := r.URL.Query()["id"][0]
	// for i := 0; i < len(testUsers); i++ {
	// 	if testUsers[i].Id == userId {
	// 		res, _ := json.Marshal(testUsers[i])
	// 		w.Write(res)
	// 		return
	// 	}
	// }
	message = fmt.Sprintf("getuser?%s",userId)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalln("Failed to send message:", err)
	}
	io.WriteString(w, "got user by id")
}

// func getUserByName(w http.ResponseWriter, r *http.Request) {
// 	userName := r.URL.Query()["name"][0]
// 	for i := 0; i < len(testUsers); i++ {
// 		if testUsers[i].Name == userName {
// 			res, _ := json.Marshal(testUsers[i])
// 			w.Write(res)
// 			return
// 		}
// 	}
// 	io.WriteString(w, "Not Found")
// }


func addUser(w http.ResponseWriter, r *http.Request){
	var u User
	err := json.NewDecoder(r.Body).Decode(&u)
    if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }
	fmt.Println(u)
	// testUsers = append(testUsers,u)

	str,err := json.Marshal(u)

	message = fmt.Sprintf("adduser?%s",str)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		log.Fatalln("Failed to send message:", err)
	}

	io.WriteString(w,"Add User")
}

func deleteUserById(w http.ResponseWriter, r *http.Request) {
	// fmt.Println(r)
	userId := r.URL.Query()["id"][0]
	// for i := 0; i < len(testUsers); i++ {
	// 	if testUsers[i].Id == userId {
	// 		testUsers = testUsers[:i+copy(testUsers[i:], testUsers[i+1:])]
	// 		fmt.Println(testUsers)
	// 		io.WriteString(w,"User Deleted")
	// 		return
	// 	}
	// }

	// str,err := json.Marshal(userId)

	message = fmt.Sprintf("deleteuser?%s",userId)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalln("Failed to send message:", err)
	}

	io.WriteString(w, "deleted user")
}

func userOperations(w http.ResponseWriter, r *http.Request){
	// fmt.Println(r.Method)
	switch r.Method {
		case "GET":
			getUserById(w,r)
		case "POST":
			addUser(w,r)
		case "DELETE":
			deleteUserById(w,r)
		default:
			http.Error(w, "Invalid request method.", 405)
	}
}

func main() {

	config := sarama.NewConfig()
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Producer.RequiredAcks = sarama.WaitForAll

	brokers := []string{"localhost:9092"}
	producer, _ = sarama.NewSyncProducer(brokers, config)
	// if err != nil {
	// 	log.Fatalln("Failed to start Sarama producer:", err)
	// }

	defer func() { _ = producer.Close() }()

	message = "Hello, Kafka!"
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalln("Failed to send message:", err)
	}

	// testUsers = []User{
	// 	{Id: uuid.New().String(), Name: "A", Address: "x"},
	// 	{Id: uuid.New().String(), Name: "B", Address: "y"},
	// 	{Id: uuid.New().String(), Name: "C", Address: "z"},
	// }
	// newUser := User{Id:uuid.New().String(),Name:"D",Address:"p"}
	// testUsers = append(testUsers, newUser)
	
	// fmt.Println("producer set up")
	
	http.HandleFunc("/", getRoot)
	http.HandleFunc("/allUser", getAllUser)
	http.HandleFunc("/user", userOperations)
	
	// fmt.Println("producer set up")
	http.ListenAndServe(":8081", nil)
	// fmt.Println("producer set up")
}