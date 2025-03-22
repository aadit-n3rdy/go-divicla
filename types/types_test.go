package types_test

import (
	"bufio"
	"bytes"
	"testing"
	"time"

	"github.com/aadit-n3rdy/go-divicla/types"
)

func TestComputeTask_ToWriterAndFromReader(t *testing.T) {
	sizes := []int{1000 * 1000}
	fbuf := make([]float32, 1000*1000)
	for i := range fbuf {
		fbuf[i] = float32(i)
	}
	originalTask := types.ComputeTask{
		ID: types.ComputeID{
			SourceID:  "test-source",
			Timestamp: time.Now(),
		},
		Data: types.Tensor{
			Sizes:  sizes,
			Buffer: fbuf, // randomly generated
		},
	}

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	originalTask.ToWriter(writer)

	reader := bufio.NewReader(&buf)
	var receivedTask types.ComputeTask
	err := receivedTask.FromReader(reader)
	if err != nil {
		t.Fatalf("Failed to read from reader: %v", err)
	}

	if originalTask.ID.SourceID != receivedTask.ID.SourceID {
		t.Errorf("SourceID mismatch: expected %v, got %v", originalTask.ID.SourceID, receivedTask.ID.SourceID)
	}

	if originalTask.ID.Timestamp.Month() != receivedTask.ID.Timestamp.Month() ||
		originalTask.ID.Timestamp.Day() != receivedTask.ID.Timestamp.Day() ||
		originalTask.ID.Timestamp.Hour() != receivedTask.ID.Timestamp.Hour() ||
		originalTask.ID.Timestamp.Minute() != receivedTask.ID.Timestamp.Minute() ||
		originalTask.ID.Timestamp.Second() != receivedTask.ID.Timestamp.Second() {
		t.Errorf("Timestamp mismatch: expected %v, got %v", originalTask.ID.Timestamp, receivedTask.ID.Timestamp)
	}

	if len(originalTask.Data.Sizes) != len(receivedTask.Data.Sizes) {
		t.Fatalf("Sizes length mismatch: expected %v, got %v", len(originalTask.Data.Sizes), len(receivedTask.Data.Sizes))
	}

	for i, size := range originalTask.Data.Sizes {
		if size != receivedTask.Data.Sizes[i] {
			t.Fatalf("Size mismatch at index %d: expected %v, got %v", i, size, receivedTask.Data.Sizes[i])
		}
	}

	if len(originalTask.Data.Buffer) != len(receivedTask.Data.Buffer) {
		t.Fatalf("Buffer length mismatch: expected %v, got %v", len(originalTask.Data.Buffer), len(receivedTask.Data.Buffer))
	}

	for i, val := range originalTask.Data.Buffer {
		if val != receivedTask.Data.Buffer[i] {
			t.Errorf("Buffer value mismatch at index %d: expected %v, got %v", i, val, receivedTask.Data.Buffer[i])
		}
	}
}
