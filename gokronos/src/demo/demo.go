/*
# GoKronos


## Introduction
The contents of this file can be found in `demo.go` and are compiled
into `README.md`, so you can consume the readme while running the
Go program (`make demo`) to understand how it works.
*/
package main

import (
	"fmt"
	"time"

	"gokronos"
)

func main() {
	/*
		Create a Kronos client with the URL of a running server. Optionally
		provide a `namespace` to explicitly work with events in a particular namespace.
		To not use a namespace, create the client with `namespace := ""`
	*/
	namespace := "demo"
	kc := gokronos.MakeKronosClient("http://127.0.0.1:8151", namespace)
	startTime := gokronos.KronosTimeNow()

	/*
		## Index Request
		The calling `kc.Index()` will return information about the running Kronos server.
	*/
	kronosResponse, _ := kc.Index()
	fmt.Printf("KronosIndex: %v\n", kronosResponse)
	/*
		## Inserting data
		Insert data with the `kc.Put()` command. The argument is a stream to
		insert the data into. You can provide a single event which holds an
		arbitrary dictionary of JSON encodeable data.
	*/
	stream := "yourproduct.website.pageviews"

	//make an event to log
	event := make(map[string]interface{})
	event["source"] = "http://test.com"

	browserEvent := make(map[string]interface{})
	browserEvent["name"] = "Chrome"
	browserEvent["version"] = 26
	event["browser"] = browserEvent

	event["pages"] = [...]string{"page1.html", "page2.html"}
	kronosResponse, _ = kc.Put(stream, event, nil) // use the client's namespace
	fmt.Printf("KronosPut: %v\n", kronosResponse)

	/*
		### Optionally add a timestamp
		By default, each event will be timestamped on the client.  If you add
		a `gokronos.TimestampField` argument, you can specify the time at which each
		event ocurred.
	*/
	optionalTime := gokronos.TimetoKronosTime(time.Date(1980, 1, 6, 0, 0, 0, 0, time.UTC))
	event[gokronos.TimestampField] = optionalTime
	kronosResponse, _ = kc.Put(stream, event, nil) // use the client's namespace
	fmt.Printf("KronosPut: %v\n", kronosResponse)

	/*
		## Retrieving data
		Retrieving data requires a `stream` name, a `startTime`, and an `endTime`.
		Note that an `IdField` and `@TimestampField` field are
		attached to each event.  The `IdField` is a UUID1-style identifier
		with its time bits derived from the timestamp.  This allows event IDs
		to be roughly sortable by the time that they happened while providing
		a deterministic tiebreaker when two events happened at the same time.
	*/
	time.Sleep(1000 * time.Millisecond)
	ch := kc.Get(stream, startTime, gokronos.KronosTimeNow(), nil)
	for kronosStream := range ch {
		fmt.Printf("Received event: %v\n", kronosStream.Response)
	}
	/*
		### Event order
		By default, events are returned in ascending order of their
		`IdField`. Pass in the optional argument `gokronos.DescendingOrder` argument to
		change this behavior to be in descending order of `IdField`.
	*/
	optionalArgs := make(map[string]interface{})
	optionalArgs["order"] = gokronos.DescendingOrder
	ch = kc.Get(stream, startTime, gokronos.KronosTimeNow(), optionalArgs)
	for kronosStream := range ch {
		fmt.Printf("Reverse event: %v\n", kronosStream.Response)
	}
	/*
		### Limiting events
		If you only want to retrieve a limited number of events, use the
		`limit` argument.
	*/
	optionalArgs = make(map[string]interface{})
	optionalArgs["limit"] = 1
	ch = kc.Get(stream, startTime, gokronos.KronosTimeNow(), optionalArgs)
	for kronosStream := range ch {
		fmt.Printf("Limited event: %v\n", kronosStream.Response)
	}

	/*
	   ## Getting a list of streams
	   To see all streams available in this namespace, use `GetStreams`.
	*/

	ch1, _ := kc.GetStreams(nil)
	for kronosResponse := range ch1 {
		fmt.Printf("Found stream %v\n", kronosResponse.Json["stream"])
	}

	/*
	   ## Deleting data
	   Sometimes, we make an oopsie and need to delete some events.  The
	   `Delete` function takes similar arguments for the start and end
	   timestamps to delete.

	   Note: The most common Kronos use cases are for write-mostly systems
	   with high-throughput reads.  As such, you can imagine that most
	   backends will not be delete-optimized.  There's nothing in the Kronos
	   API that inherently makes deletes not performant, but we imagine some
	   backends will make tradeoffs to optimize their write and read paths at
	   the expense of fast deletes.
	*/
	kronosResponse, _ = kc.Delete(stream, startTime, gokronos.KronosTimeNow(), nil)
	backend := kronosResponse.Json[stream].(map[string]interface{})["memory"]
	numDeleted := backend.(map[string]interface{})["num_deleted"].(float64)
	fmt.Printf("Deleted %d events\n", int(numDeleted))
}
