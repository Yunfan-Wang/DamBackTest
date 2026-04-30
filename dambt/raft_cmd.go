package dambt

import (
	"bytes"
	"encoding/gob"
)

type DAMBTCommand struct {
	Op     string     `json:"op"`
	Chunk  *ChunkMeta `json:"chunk,omitempty"`
	Job    *JobSpec   `json:"job,omitempty"`
	JobID  JobID      `json:"job_id,omitempty"`
	Status JobStatus  `json:"status,omitempty"`
	Result *JobResult `json:"result,omitempty"`
}

const (
	CmdRegisterChunk = "REGISTER_CHUNK"
	CmdSubmitJob     = "SUBMIT_JOB"
	CmdJobStatus     = "JOB_STATUS"
	CmdJobDone       = "JOB_DONE"
)

func init() {
	gob.Register(DAMBTCommand{})
	gob.Register(ChunkMeta{})
	gob.Register(JobSpec{})
	gob.Register(JobResult{})
	gob.Register(JobStatus(""))
}

func EncodeCommand(cmd DAMBTCommand) []byte {
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(cmd)
	return buf.Bytes()
}

func DecodeCommand(data []byte) (DAMBTCommand, error) {
	var cmd DAMBTCommand
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd)
	return cmd, err
}