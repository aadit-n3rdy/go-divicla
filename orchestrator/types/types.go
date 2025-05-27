package types

type RegSourceReq struct {
	SrcID string
	Addr  string
}

type RegCompReq struct {
	Addr     string
	Capacity float32 // in number of commitments
}

type SetDeficitReq struct {
	SrcID   string
	Deficit float32
}

type OrcNode struct {
	ID      string
	Deficit float32
	Addr    string
}

type SetUtilReq struct {
	ComputeID string
	Util      float32
}

type CompNode struct {
	Capacity   float32
	Util       float32
	Commitment float32
}
