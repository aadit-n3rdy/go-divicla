package main

import (
	"time"

	"github.com/aadit-n3rdy/go-divicla/types"
)

type Cache struct {
	count     int32
	maxSize   int32
	tasks     []types.ComputeTask
	headIndex int32
}

func (c *Cache) Init(maxSize int32) {
	c.maxSize = maxSize
	c.headIndex = 0
}

func copyTensor(dest *types.Tensor, src *types.Tensor) {
	if len(dest.Sizes) != len(src.Sizes) {
		dest.Sizes = make([]int, len(src.Sizes))
	}
	for i := range src.Sizes {
		dest.Sizes[i] = src.Sizes[i]
	}

	if len(dest.Buffer) != len(src.Buffer) {
		dest.Buffer = make([]float32, len(src.Buffer))
	}
	for i := range src.Buffer {
		dest.Buffer[i] = src.Buffer[i]
	}
}

func (c *Cache) Insert(task *types.ComputeTask) {
	c.tasks[c.headIndex].ID = task.ID
	copyTensor(&c.tasks[c.headIndex].Data, &task.Data)
	c.headIndex = (c.headIndex + 1) % c.maxSize
}

func (c *Cache) Get(ts time.Time) *types.ComputeTask {
	for i := range c.tasks {
		if c.tasks[i].ID.Timestamp.Equal(ts) {
			return &c.tasks[i]
		}
	}
	return nil
}
