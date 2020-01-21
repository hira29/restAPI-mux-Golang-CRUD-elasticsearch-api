package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"gopkg.in/olivere/elastic.v6"
	"math/rand"
	"net/http"
	"os"
	"time"
)

const index_name string = "massive-profiling"

type users struct {
	Name          string `json:"name"`
	Location      string `json:"location"`
	Location_type string `json:"location_type"`
}

func justAnAPI() {
	fmt.Println("API Started!")
	r := mux.NewRouter()
	s := r.PathPrefix("/API/").Subrouter()

	s.HandleFunc("/search", searchData).Methods("GET", "POST")

	_ = http.ListenAndServe(":8080", r)
}

func elasticConnection() *elastic.Client {

	client, err := elastic.NewClient(elastic.SetURL("http://192.168.114.111:9200"))
	if err != nil {
		fmt.Println("ERROR!")
		os.Exit(-1)
	}
	return client
}

func elasticCheck() {
	//Cek Keberadaan
	fmt.Println("Connecting to elastic")
	client := elasticConnection()

	exists, err := client.IndexExists(index_name).Do(context.Background())
	if err != nil {
		fmt.Println("ERROR! : Can't Find any Index")
		os.Exit(-1)
	}
	if !exists {
		fmt.Println("No Index Detected!")
	}

	fmt.Println("Connected to Elastic!")
}

func elasticSearch(post users) map[string]interface{} {
	client := elasticConnection()
	//Testing for Search in Elasticsearch
	var location string
	location = "location_default." + post.Location_type

	termQuery := elastic.NewBoolQuery().
		Must(elastic.NewTermQuery(location, post.Location)).
		Must(elastic.NewWildcardQuery("name", post.Name))

	aggregate_gender := elastic.NewTermsAggregation().Field("gender.keyword").OrderByKeyAsc()
	aggregate_age_range := elastic.NewTermsAggregation().Field("age_range.keyword").OrderByKeyAsc()

	searchResult, err := client.Search().
		Index(index_name).
		Aggregation("gender", aggregate_gender).
		Aggregation("age_range", aggregate_age_range).
		Query(termQuery).
		From(0).Size(10).
		Pretty(true).
		Do(context.Background())

	if err != nil {
		// Handle error
		panic(err)
	}

	gender_data := make(map[string]interface{})
	age_range_data := make(map[string]interface{})

	gender, _ := searchResult.Aggregations.Terms("gender")
	age_range, _ := searchResult.Aggregations.Terms("age_range")

	for _, bucket := range gender.Buckets {
		str1 := fmt.Sprintf("%v", bucket.Key)
		str2 := fmt.Sprintf("%v", bucket.DocCount)
		gender_data[str1] = str2
	}

	for _, bucket := range age_range.Buckets {
		str1 := fmt.Sprintf("%v", bucket.Key)
		str2 := fmt.Sprintf("%v", bucket.DocCount)
		age_range_data[str1] = str2
	}

	getdata := make(map[string]interface{})

	getdata["gender"] = gender_data
	getdata["age_range"] = age_range_data

	response := make(map[string]interface{})

	if post.Name == "" || post.Location_type == "" || post.Location == "" {
		response["message"] = "failed"
		response["data"] = nil
		response["status"] = false
	} else {
		response["message"] = "success"
		response["data"] = getdata
		response["status"] = true
	}
	return response
}

func searchData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var post users
	_ = json.NewDecoder(r.Body).Decode(&post)
	_ = json.NewEncoder(w).Encode(elasticSearch(post))
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	//Elastic Check
	elasticCheck()
	//API Connection
	justAnAPI()
}
