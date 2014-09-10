package gokronos

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

// These are constants, do not modify them.
const IdField = "@id"
const TimestampField = "@timestamp"
const LibraryField = "@library"
const SuccessField = "@success"
const AscendingOrder = "ascending"
const DescendingOrder = "descending"

type KronosClient struct {
	indexUrl       string
	putUrl         string
	getUrl         string
	deleteUrl      string
	streamsUrl     string
	inferSchemaUrl string
	namespace      string
}

type KronosResponse struct {
	Json map[string]interface{}
}

type KronosError struct {
	Err     error
	Message string
}

type KronosStreamResponse struct {
	Response *KronosResponse
	Error    *KronosError
}

func (ke *KronosError) Error() string {
	return fmt.Sprintf("[KronosError] %s %s", ke.Message, ke.Err.Error())
}

// TODO(jblum): blocking and non blocking clients
func MakeKronosClient(httpUrl string, namespace string) *KronosClient {
	kc := new(KronosClient)
	kc.indexUrl = fmt.Sprintf("%s/1.0/index", httpUrl)
	kc.putUrl = fmt.Sprintf("%s/1.0/events/put", httpUrl)
	kc.getUrl = fmt.Sprintf("%s/1.0/events/get", httpUrl)
	kc.deleteUrl = fmt.Sprintf("%s/1.0/events/delete", httpUrl)
	kc.streamsUrl = fmt.Sprintf("%s/1.0/streams", httpUrl)
	kc.inferSchemaUrl = fmt.Sprintf("%s/1.0/streams/infer_schema", httpUrl)
	kc.namespace = namespace
	return kc
}

func (kc *KronosClient) Error(err error, message string) *KronosError {
	return &KronosError{err, message}
}

func (kc *KronosClient) Index() (*KronosResponse, *KronosError) {
	resp, err := http.Get(kc.indexUrl)
	if err != nil {
		err := kc.Error(err, "Error fetching index: ")
		return nil, err
	}
	return kc.parseKronosResponse(resp)
}

/*
	`stream`: Stream to put the event in.
	`event`: The event object.
	`namespace` (optional): Namespace for the stream.
*/
// TODO(jblum): Implement put mulitiple events
func (kc *KronosClient) Put(stream string, event map[string]interface{}, options map[string]interface{}) (*KronosResponse, *KronosError) {

	if options == nil {
		options = make(map[string]interface{})
	}
	data := make(map[string]interface{})
	kc.setNamespace(options, data)

	if event == nil {
		event = make(map[string]interface{})
	}

	if event[TimestampField] == nil {
		event[TimestampField] = KronosTimeNow()
	}
	event[LibraryField] = map[string]string{
		"name":    ClientName,
		"version": ClientVersion,
	}

	events := make(map[string]interface{})
	streams := make([]interface{}, 1)
	streams[0] = event
	events[stream] = streams
	data["events"] = events

	return kc.makeRequest(kc.putUrl, data)
}

/*
	Queries a stream with name `stream` for all events between `startTime` and
    `endTime` (both inclusive).  An optional `startId` allows the client to
    restart from a failure, specifying the last ID they read; all events that
    happened after that ID will be returned. An optional `limit` limits the
    maximum number of events returned.  An optional `order` requests results in
    `AscendingOrder` or `DescendingOrder`.
*/
// TODO(jblum): Do date parsing similar to Python client to be more flexible
func (kc *KronosClient) Get(stream string, startTime *KronosTime, endTime *KronosTime, options map[string]interface{}) chan *KronosStreamResponse {
	requestDict := make(map[string]interface{})
	requestDict["stream"] = stream
	requestDict["end_time"] = endTime.Time

	if options == nil {
		options = make(map[string]interface{})
	}

	//parse options
	kc.setStart(options, startTime, requestDict)
	kc.setNamespace(options, requestDict)

	limit := options["limit"]
	if limit != nil {
		requestDict["limit"] = limit.(int)
	}

	order := options["order"]
	if order == nil {
		order = AscendingOrder
	} else {
		order = order.(string)
	}
	requestDict["order"] = order

	ch := make(chan *KronosStreamResponse)
	go func() {
		errorCount := 0
		maxErrors := 10
		lastId := ""
	Retry:
		for {
			// TODO(jblum): stream response!
			resp, err := kc.postJson(kc.getUrl, requestDict)
			if err != nil {
				errorCount++
				if lastId != "" {
					delete(requestDict, "start_time")
					requestDict["start_id"] = lastId
				}
				if errorCount == maxErrors {
					ch <- &KronosStreamResponse{nil, err}
					close(ch)
					break Retry
				}
			} else {
				dec := json.NewDecoder(resp.Body)
				for {
					var event map[string]interface{}
					if err := dec.Decode(&event); err == io.EOF {
						close(ch)
						break Retry
					} else if err == nil {
						lastId = event[IdField].(string)
						kronosResponse := KronosResponse{event}
						ch <- &KronosStreamResponse{&kronosResponse, nil}
					}
				}
			}
		}
	}()
	return ch
}

/*
   Delete events in the stream with name `stream` that occurred between
   `startTime` and `endTime` (both inclusive).  An optional `startId` allows
   the client to delete events starting from after an ID rather than starting
   at a timestamp.
*/
// TODO(jblum): Do date parsing similar to Python client to be more flexible
func (kc *KronosClient) Delete(stream string, startTime *KronosTime, endTime *KronosTime, options map[string]interface{}) (*KronosResponse, *KronosError) {
	if options == nil {
		options = make(map[string]interface{})
	}

	requestDict := make(map[string]interface{})
	requestDict["stream"] = stream
	requestDict["end_time"] = endTime.Time

	kc.setStart(options, startTime, requestDict)
	kc.setNamespace(options, requestDict)

	return kc.makeRequest(kc.deleteUrl, requestDict)
}

/*
 Queries the Kronos server and fetches a list
 of streams available to be read.
*/
func (kc *KronosClient) GetStreams(options map[string]interface{}) (chan *KronosResponse, *KronosError) {
	if options == nil {
		options = make(map[string]interface{})
	}

	requestDict := make(map[string]interface{})
	kc.setNamespace(options, requestDict)

	resp, err := kc.postJson(kc.streamsUrl, requestDict)

	if err != nil {
		return nil, err
	}

	// TODO(jblum): abstract this into a streaming function
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	ch := make(chan *KronosResponse)

	go func() {
		splits := strings.Split(string(body[:]), "\r\n")
		for _, stream := range splits {
			if stream == "" {
				continue
			}
			event := make(map[string]interface{})
			event["stream"] = stream
			kronosResponse := KronosResponse{event}
			ch <- &kronosResponse
		}
		close(ch)
	}()

	return ch, nil
}

func (kc *KronosClient) InferSchema(stream string, options map[string]interface{}) (*KronosResponse, *KronosError) {
	if options == nil {
		options = make(map[string]interface{})
	}

	requestDict := make(map[string]interface{})
	requestDict["stream"] = stream
	kc.setNamespace(options, requestDict)

	return kc.makeRequest(kc.inferSchemaUrl, requestDict)
}

/*
	Helper function to either `startId` or `startTime` in the requestDict.
*/
func (kc *KronosClient) setStart(options map[string]interface{}, startTime *KronosTime, requestDict map[string]interface{}) {
	startId := options["startId"]
	if startId == nil {
		requestDict["start_time"] = startTime.Time
	} else {
		requestDict["start_id"] = startId.(string)
	}
}

/*
	Helper function to set `namespace` in the requestDict.
	Tries to use options and falls back to the client level namespace, if present.
*/
func (kc *KronosClient) setNamespace(options map[string]interface{}, requestDict map[string]interface{}) {
	namespace := options["namespace"]
	if namespace == nil {
		namespace = kc.namespace
	} else {
		namespace = namespace.(string)
	}

	if namespace != "" {
		requestDict["namespace"] = namespace
	}
}

func (kc *KronosClient) jsonMarshal(data interface{}) ([]byte, *KronosError) {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		err := kc.Error(err, "Error with JSON Marshal: ")
		return nil, err
	}
	return jsonBytes, nil
}

/*
	Helper function which tries to parse a `[]byte` array of JSON
	data and return a parsed `http.Response` object
*/
func (kc *KronosClient) postJson(url string, data interface{}) (*http.Response, *KronosError) {
	jsonBytes, e := kc.jsonMarshal(data)

	if e != nil {
		return nil, e
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonBytes))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		err := kc.Error(err, "Unable to make request")
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		return nil, kc.Error(errors.New(""), fmt.Sprintf("Bad server response code %d", resp.StatusCode))
	}
	return resp, nil
}

func (kc *KronosClient) makeRequest(url string, data interface{}) (*KronosResponse, *KronosError) {
	resp, err := kc.postJson(url, data)

	if err != nil {
		return nil, err
	}
	return kc.parseKronosResponse(resp)
}

/*
	Helper function to parse an HTTP response
	into a KronosResponse. Verifies that the `@success` field is true
*/
func (kc *KronosClient) parseKronosResponse(resp *http.Response) (*KronosResponse, *KronosError) {
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	var dat map[string]interface{}
	if err := json.Unmarshal(body, &dat); err != nil {
		err := kc.Error(err, "Error unmarshalling JSON data")
		return nil, err
	}

	if !dat[SuccessField].(bool) {
		err := kc.Error(nil, fmt.Sprintf("Encountered errors with request: %v", dat))
		return nil, err
	}
	return &KronosResponse{dat}, nil
}
