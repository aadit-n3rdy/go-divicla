package types

import (
	"bufio"
	"fmt"
	"strings"
	"time"
	"unsafe"
)

type Tensor struct {
	Sizes  []int
	Buffer []float32
}

type ComputeResult struct {
	ID     ComputeID
	Result Tensor
}

type ComputeID struct {
	SourceID  string
	Timestamp time.Time
}

type ComputeTask struct {
	ID   ComputeID
	Data Tensor
}

func (ct *ComputeTask) ToWriter(wr *bufio.Writer) error {
	_, err := wr.WriteString(ct.ID.SourceID + "\n")
	if err != nil {
		fmt.Println("Error writing source ID to writer: ", err)
		return err
	}
	_, err = wr.WriteString(ct.ID.Timestamp.Format(time.Stamp) + "\n")
	if err != nil {
		fmt.Println("Error writing timestamp to writer: ", err)
		return err
	}
	for _, val := range ct.Data.Sizes {
		_, err = wr.WriteString(fmt.Sprint(val) + " ")
		if err != nil {
			fmt.Println("Error writing tensor size to writer: ", err)
			return err
		}
	}
	wr.WriteString("\n")
	bytePointer := unsafe.Pointer(&ct.Data.Buffer[0])
	byteBuffer := unsafe.Slice((*byte)(bytePointer), len(ct.Data.Buffer)*4)
	_, err = wr.Write(byteBuffer)
	if err != nil {
		fmt.Println("Error writing tensor buffer to writer: ", err)
		return err
	}
	err = wr.Flush()
	return err
}

func (ct *ComputeTask) FromReader(rd *bufio.Reader) error {
	ct.ID.SourceID, _ = rd.ReadString('\n')
	ct.ID.SourceID = strings.TrimSpace(ct.ID.SourceID)
	tsString, _ := rd.ReadString('\n')
	tsString = strings.TrimSpace(tsString)
	var err error
	ct.ID.Timestamp, err = time.Parse(time.Stamp, tsString)
	if err != nil {
		return err
	}
	ct.Data.Sizes = make([]int, 0)
	szLine, _ := rd.ReadString('\n')
	trimmed := strings.TrimSpace(szLine)
	szStrs := strings.Split(trimmed, " ")
	count := 1
	for _, val := range szStrs {
		var sz int
		fmt.Sscan(val, &sz)
		ct.Data.Sizes = append(ct.Data.Sizes, sz)
		count *= sz
	}
	for i := 0; i < count; i++ {
		byteBuf := make([]byte, 4)
		byteBuf[0], _ = rd.ReadByte()
		byteBuf[1], _ = rd.ReadByte()
		byteBuf[2], _ = rd.ReadByte()
		byteBuf[3], _ = rd.ReadByte()
		floatPtr := (*float32)(unsafe.Pointer(&byteBuf[0]))
		ct.Data.Buffer = append(ct.Data.Buffer, *floatPtr)
	}

	//byteBuf := make([]byte, count*4)
	//_, err = rd.Read(byteBuf)
	//if err != nil {
	//	fmt.Println("Error reading tensor buffer from reader: ", err)
	//	return err
	//}
	//floatPtr := (*float32)(unsafe.Pointer(&byteBuf[0]))
	//ct.Data.Buffer = unsafe.Slice(floatPtr, count)
	return nil
}
