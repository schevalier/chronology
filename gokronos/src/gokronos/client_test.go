package gokronos

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const sleepTime = 1000 * time.Millisecond

func setupClient() *KronosClient {
	return MakeKronosClient("http://127.0.0.1:8151", "kronos")
}

func TestIndex(t *testing.T) {
	kc := setupClient()
	_, err := kc.Index()
	assertNoError(t, "Index", err)
}

func TestPut(t *testing.T) {
	kc := setupClient()
	stream := "test_put"
	event := make(map[string]interface{})
	event["test_event"] = 1
	optionalArgs := make(map[string]interface{})
	optionalArgs["namespace"] = "kronos"
	kronosResponse, err := kc.Put(stream, event, optionalArgs)
	assertNoError(t, "putWithValues", err)
	assertPut(t, kronosResponse)

	// nil event, nil optionalArgs
	kronosResponse, err = kc.Put(stream, nil, nil)
	assertNoError(t, "putWithEmpty", err)
	assertPut(t, kronosResponse)
}

func TestPutAndGet(t *testing.T) {
	kc := setupClient()

	startTime := KronosTimeNow()
	stream := "test_put_and_get"

	eventCount := 5
	for i := 0; i < eventCount; i++ {
		event := make(map[string]interface{})
		event[fmt.Sprintf("test_event-%d", i)] = i
		kc.Put(stream, event, nil)
	}

	time.Sleep(sleepTime)
	endTime := KronosTimeNow()

	testTag := "testGet"
	ch := kc.Get(stream, startTime, endTime, nil)
	getAndAssert(t, ch, testTag, eventCount)

	testTag = "testGetOptionalArgs"
	limitCount := 2
	optionalArgs := make(map[string]interface{})
	optionalArgs["limit"] = limitCount
	optionalArgs["order"] = AscendingOrder

	ch = kc.Get(stream, startTime, endTime, optionalArgs)
	count := 0
	lastId := ""

	for kronosStream := range ch {
		assertNoError(t, testTag, kronosStream.Error)
		kronosResponse := kronosStream.Response
		count++
		assertGet(t, kronosResponse)
		curId := kronosResponse.Json[IdField].(string)
		if lastId != "" {
			if lastId > curId {
				t.Fatalf("testGetAscendingOrder failed, got %v, %v", lastId, curId)
			}
		}
		lastId = curId
	}
	assert.Equal(t, limitCount, count, testTag)

	testTag = "testStartId"
	optionalArgs = make(map[string]interface{})
	optionalArgs["startId"] = lastId

	ch = kc.Get(stream, startTime, endTime, optionalArgs)
	getAndAssert(t, ch, testTag, eventCount-limitCount)
}

func TestDelete(t *testing.T) {
	kc := setupClient()

	startTime := KronosTimeNow()
	stream := "test_delete"

	putCount1 := 2
	putMany(kc, stream, putCount1)

	time.Sleep(sleepTime)
	midTime := KronosTimeNow()

	putCount2 := 3
	putMany(kc, stream, putCount2)

	time.Sleep(sleepTime)
	endTime := KronosTimeNow()

	testTag := "testDelete_get1"
	ch := kc.Get(stream, startTime, endTime, nil)
	getAndAssert(t, ch, testTag, putCount1+putCount2)

	kronosResponse, err := kc.Delete(stream, startTime, midTime, nil)
	assertNoError(t, testTag, err)
	backend := kronosResponse.Json[stream].(map[string]interface{})["memory"]
	numDeleted := backend.(map[string]interface{})["num_deleted"].(float64)

	assert.Equal(t, putCount1, int(numDeleted), "testDelete1")

	testTag = "testDelete_get2"
	ch = kc.Get(stream, startTime, endTime, nil)
	getAndAssert(t, ch, testTag, putCount2)

	//TODO(jblum): test Delete() with optionalArgs
}

func TestGetStreams(t *testing.T) {
	kc := setupClient()
	streamCount := 5

	streamName := "stream-"
	for i := 0; i < streamCount; i++ {
		kc.Put(fmt.Sprintf("%s%d", streamName, i), nil, nil)
	}

	time.Sleep(sleepTime)
	ch, err := kc.GetStreams(nil)

	assertNoError(t, "testGetStreams", err)
	count := 0
	for kronosResponse := range ch {
		if strings.Contains(kronosResponse.Json["stream"].(string), streamName) {
			count++
			assertGet(t, kronosResponse)
		}
	}
	assert.Equal(t, streamCount, count, "testGetStreams")
}

func TestInferSchema(t *testing.T) {
	kc := setupClient()
	stream := "infer_schema_stream"
	test_tag := "test_infer_schema"
	event := make(map[string]interface{})
	event["a"] = 1
	event[TimestampField] = 1
	kc.Put(stream, event, nil)

	event["a"] = 2.3
	event[TimestampField] = 2
	event["optional"] = false
	kc.Put(stream, event, nil)

	time.Sleep(sleepTime)
	kronosResponse, err := kc.InferSchema(stream, nil)
	assertNoError(t, test_tag, err)

	assert.Equal(t, kronosResponse.Json["stream"].(string), stream, "Correct Schema")

	schema := kronosResponse.Json["schema"].(map[string]interface{})
	properties := schema["properties"].(map[string]interface{})

	assert.Equal(t, properties[IdField].(map[string]interface{})["type"].(string), "string", "Correct IdField")
	assert.Equal(t, properties[TimestampField].(map[string]interface{})["type"].(string), "integer", "Correct TimestampField")
	assert.Equal(t, properties[TimestampField].(map[string]interface{})["type"].(string), "integer", "Correct TimestampField")

	assert.Equal(t, properties["a"].(map[string]interface{})["type"].(string), "number", "Correct Property: a")
	assert.Equal(t, properties["optional"].(map[string]interface{})["type"].(string), "boolean", "Correct Property: optional")
}

func putMany(kc *KronosClient, stream string, putCount int) {
	for i := 0; i < putCount; i++ {
		event := make(map[string]interface{})
		event[fmt.Sprintf("test_event-%d", i)] = i
		kc.Put(stream, event, nil)
	}
}

func getAndAssert(t *testing.T, ch chan *KronosStreamResponse, testTag string, expectedCount int) {
	count := 0
	for kronosStream := range ch {
		assertNoError(t, testTag, kronosStream.Error)
		count++
		assertGet(t, kronosStream.Response)
	}
	assert.Equal(t, expectedCount, count, testTag)
}

func assertNoError(t *testing.T, requestName string, err *KronosError) {
	if err != nil {
		t.Fatalf(fmt.Sprintf("Error performing %s() request %v", requestName, err))
	}
}

func assertPut(t *testing.T, kronosResponse *KronosResponse) {
	if len(kronosResponse.Json) < 2 {
		t.Fatalf("No events inserted %v", kronosResponse.Json)
	}
}

func assertGet(t *testing.T, kronosResponse *KronosResponse) {
	if len(kronosResponse.Json) == 0 {
		t.Fatalf("Error with get, received: %v", kronosResponse)
	}
}
