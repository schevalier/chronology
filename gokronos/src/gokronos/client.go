package gokronos

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

// These are constants, do not modify them.
const IdField = "@id"
const TimestampField = "@timestamp"
const SuccessField = "@success"
const AscendingOrder = "ascending"
const DescendingOrder = "descending"

type KronosClient struct {
	indexUrl   string
	putUrl     string
	getUrl     string
	deleteUrl  string
	streamsUrl string
	namespace  string
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
func (kc *KronosClient) Put(stream string, event map[string]interface{}, optionalArgs map[string]interface{}) (*KronosResponse, *KronosError) {

	if optionalArgs == nil {
		optionalArgs = make(map[string]interface{})
	}
	// expand optional args
	namespace := optionalArgs["namespace"]
	if namespace == nil {
		namespace = kc.namespace
	} else {
		namespace = namespace.(string)
	}

	if event == nil {
		event = make(map[string]interface{})
	}
	if event[TimestampField] == nil {
		event[TimestampField] = KronosTimeNow()

	}

	data := make(map[string]interface{})
	if namespace != "" {
		data["namespace"] = namespace
	}

	events := make(map[string]interface{})
	streams := make([]interface{}, 1)
	streams[0] = event
	events[stream] = streams
	data["events"] = events

	jsonBytes, err := kc.jsonMarshal(data)
	if err != nil {
		return nil, err
	}
	resp, err := kc.postJson(kc.putUrl, jsonBytes)
	if err != nil {
		return nil, err
	}
	return kc.parseKronosResponse(resp)
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
func (kc *KronosClient) Get(stream string, startTime *KronosTime, endTime *KronosTime, optionalArgs map[string]interface{}) chan *KronosStreamResponse {
	requestDict := make(map[string]interface{})
	requestDict["stream"] = stream
	requestDict["end_time"] = endTime.Time

	if optionalArgs == nil {
		optionalArgs = make(map[string]interface{})
	}

	//parse optionalArgs
	startId := optionalArgs["startId"]
	if startId == nil {
		requestDict["start_time"] = startTime.Time
	} else {
		requestDict["start_id"] = startId.(string)
	}

	limit := optionalArgs["limit"]
	if limit != nil {
		requestDict["limit"] = limit.(int)
	}

	order := optionalArgs["order"]
	if order == nil {
		order = AscendingOrder
	} else {
		order = order.(string)
	}
	requestDict["order"] = order

	namespace := optionalArgs["namespace"]
	if namespace == nil {
		namespace = kc.namespace
	} else {
		namespace = namespace.(string)
	}

	if namespace != "" {
		requestDict["namespace"] = namespace
	}

	ch := make(chan *KronosStreamResponse)
	go func() {
		errorCount := 0
		maxErrors := 10
		lastId := ""
	Retry:
		for {
			jsonBytes, err := kc.jsonMarshal(requestDict)
			if err != nil {
				ch <- &KronosStreamResponse{nil, err}
				break Retry
			}
			// TODO(jblum): stream response!
			resp, err := kc.postJson(kc.getUrl, jsonBytes)
			if err != nil {
				errorCount++
				if lastId != "" {
					delete(requestDict, "start_time")
					requestDict["start_id"] = lastId
				}
				if errorCount == maxErrors {
					ch <- &KronosStreamResponse{nil, err}
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
 Queries the Kronos server and fetches a list
 of streams available to be read.
*/
func (kc *KronosClient) GetStreams(optionalArgs map[string]interface{}) (chan *KronosResponse, *KronosError) {
	/*
	   Delete events in the stream with name `stream` that occurred between
	   `start_time` and `end_time` (both inclusive).  An optional `start_id` allows
	   the client to delete events starting from after an ID rather than starting
	   at a timestamp.
	*/
	if optionalArgs == nil {
		optionalArgs = make(map[string]interface{})
	}

	requestDict := make(map[string]interface{})
	namespace := optionalArgs["namespace"]
	if namespace == nil {
		namespace = kc.namespace
	} else {
		namespace = namespace.(string)
	}

	if namespace != "" {
		requestDict["namespace"] = namespace
	}

	jsonBytes, err := kc.jsonMarshal(requestDict)
	if err != nil {
		return nil, err
	}
	// TODO(jblum): implement streaming of response
	resp, err := kc.postJson(kc.streamsUrl, jsonBytes)
	if err != nil {
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		return nil, kc.Error(errors.New(""), fmt.Sprintf("Bad server response code %d", resp.StatusCode))
	}

	// TODO(jblum): abstract this into a streaming function
	ch := make(chan *KronosResponse)

	dec := json.NewDecoder(resp.Body)
	go func() {
		for {
			var stream string
			if err := dec.Decode(&stream); err == io.EOF {
				close(ch)
				break
			} else if err == nil {
				event := make(map[string]interface{})
				event["stream"] = stream
				kronosResponse := KronosResponse{event}
				ch <- &kronosResponse
			}
		}
	}()
	return ch, nil
}

// TODO(jblum): Do date parsing similar to Python client to be more flexible
func (kc *KronosClient) Delete(stream string, startTime *KronosTime, endTime *KronosTime, optionalArgs map[string]interface{}) (*KronosResponse, *KronosError) {
	if optionalArgs == nil {
		optionalArgs = make(map[string]interface{})
	}

	requestDict := make(map[string]interface{})
	requestDict["stream"] = stream
	requestDict["end_time"] = endTime.Time

	//parse optionalArgs
	startId := optionalArgs["startId"]
	if startId == nil {
		requestDict["start_time"] = startTime.Time
	} else {
		requestDict["start_id"] = startId.(string)
	}

	namespace := optionalArgs["namespace"]
	if namespace == nil {
		namespace = kc.namespace
	} else {
		namespace = namespace.(string)
	}

	if namespace != "" {
		requestDict["namespace"] = namespace
	}

	jsonBytes, err := kc.jsonMarshal(requestDict)
	if err != nil {
		return nil, err
	}
	resp, err := kc.postJson(kc.deleteUrl, jsonBytes)
	if err != nil {
		return nil, err
	}
	return kc.parseKronosResponse(resp)
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
func (kc *KronosClient) postJson(url string, jsonBytes []byte) (*http.Response, *KronosError) {
	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonBytes))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		err := kc.Error(err, "Unable to make request")
		return nil, err
	}
	return resp, nil

}

/*
	Helper function to parse an HTTP response
	into a KronosResponse. Verifies that the `@success` field is true
*/
func (kc *KronosClient) parseKronosResponse(resp *http.Response) (*KronosResponse, *KronosError) {
	//TODO check http status code
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
