package types

type RegSourceReq struct {
	SrcID string
	Addr  string
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
