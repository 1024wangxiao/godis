package cluster

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/hdt3213/godis/redis/protocol"
)

// marshalSlotIds serializes slot ids
// For example, 1, 2, 3, 5, 7, 8 -> 1-3, 5, 7-8
func marshalSlotIds(slots []*Slot) []string {
	sort.Slice(slots, func(i, j int) bool {
		return slots[i].ID < slots[j].ID
	})
	// find continuous scopes
	var scopes [][]uint32
	buf := make([]uint32, 2)
	var scope []uint32
	for i, slot := range slots {
		if len(scope) == 0 { // outside scope
			if i+1 < len(slots) &&
				slots[i+1].ID == slot.ID+1 { // if continuous, then start one
				scope = buf
				scope[0] = slot.ID
			} else { // discrete number
				scopes = append(scopes, []uint32{slot.ID})
			}
		} else { // within a scope
			if i == len(slots)-1 || slots[i+1].ID != slot.ID+1 { // reach end or not continuous, stop current scope
				scope[1] = slot.ID
				scopes = append(scopes, []uint32{scope[0], scope[1]})
				scope = nil
			}
		}

	}

	// marshal scopes
	result := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		if len(scope) == 1 {
			s := strconv.Itoa(int(scope[0]))
			result = append(result, s)
		} else { // assert len(scope) == 2
			beg := strconv.Itoa(int(scope[0]))
			end := strconv.Itoa(int(scope[1]))
			result = append(result, beg+"-"+end)
		}
	}
	return result
}

// unmarshalSlotIds deserializes lines generated by marshalSlotIds
func unmarshalSlotIds(args []string) ([]uint32, error) {
	var result []uint32
	for i, line := range args {
		if pivot := strings.IndexByte(line, '-'); pivot > 0 {
			// line is a scope
			beg, err := strconv.Atoi(line[:pivot])
			if err != nil {
				return nil, fmt.Errorf("illegal at slot line %d", i+1)
			}
			end, err := strconv.Atoi(line[pivot+1:])
			if err != nil {
				return nil, fmt.Errorf("illegal at slot line %d", i+1)
			}
			for j := beg; j <= end; j++ {
				result = append(result, uint32(j))
			}
		} else {
			// line is a number
			v, err := strconv.Atoi(line)
			if err != nil {
				return nil, fmt.Errorf("illegal at slot line %d", i)
			}
			result = append(result, uint32(v))
		}
	}
	return result, nil
}

type nodePayload struct {
	ID       string   `json:"id"`
	Addr     string   `json:"addr"`
	SlotDesc []string `json:"slotDesc"`
	Flags    uint32   `json:"flags"`
}

func marshalNodes(nodes map[string]*Node) [][]byte {
	var args [][]byte
	for _, node := range nodes {
		slotLines := marshalSlotIds(node.Slots)
		payload := &nodePayload{
			ID:       node.ID,
			Addr:     node.Addr,
			SlotDesc: slotLines,
			Flags:    node.Flags,
		}
		bin, _ := json.Marshal(payload)
		args = append(args, bin)
	}
	return args
}

func unmarshalNodes(args [][]byte) (map[string]*Node, error) {
	nodeMap := make(map[string]*Node)
	for i, bin := range args {
		payload := &nodePayload{}
		err := json.Unmarshal(bin, payload)
		if err != nil {
			return nil, fmt.Errorf("unmarshal node failed at line %d: %v", i+1, err)
		}
		slotIds, err := unmarshalSlotIds(payload.SlotDesc)
		if err != nil {
			return nil, err
		}
		node := &Node{
			ID:    payload.ID,
			Addr:  payload.Addr,
			Flags: payload.Flags,
		}
		for _, slotId := range slotIds {
			node.Slots = append(node.Slots, &Slot{
				ID:     slotId,
				NodeID: node.ID,
				Flags:  0,
			})
		}
		nodeMap[node.ID] = node
	}
	return nodeMap, nil
}

// genSnapshot
// invoker provide lock
func (raft *Raft) makeSnapshot() [][]byte {
	topology := marshalNodes(raft.nodes)
	snapshot := [][]byte{
		[]byte(raft.selfNodeID),
		[]byte(strconv.Itoa(int(raft.state))),
		[]byte(raft.leaderId),
		[]byte(strconv.Itoa(raft.term)),
		[]byte(strconv.Itoa(raft.committedIndex)),
	}
	snapshot = append(snapshot, topology...)
	return snapshot
}

// makeSnapshotForFollower used by leader node to generate snapshot for follower
// invoker provide with lock
func (raft *Raft) makeSnapshotForFollower(followerId string) [][]byte {
	snapshot := raft.makeSnapshot()
	snapshot[0] = []byte(followerId)
	snapshot[1] = []byte(strconv.Itoa(int(follower)))
	return snapshot
}

// invoker provide with lock
func (raft *Raft) loadSnapshot(snapshot [][]byte) protocol.ErrorReply {
	// make sure raft.slots and node.Slots is the same object
	selfNodeId := string(snapshot[0])
	state0, err := strconv.Atoi(string(snapshot[1]))
	if err != nil {
		return protocol.MakeErrReply("illegal state: " + string(snapshot[1]))
	}
	state := raftState(state0)
	if _, ok := stateNames[state]; !ok {
		return protocol.MakeErrReply("unknown state: " + strconv.Itoa(int(state)))
	}
	leaderId := string(snapshot[2])
	term, err := strconv.Atoi(string(snapshot[3]))
	if err != nil {
		return protocol.MakeErrReply("illegal term: " + string(snapshot[3]))
	}
	commitIndex, err := strconv.Atoi(string(snapshot[4]))
	if err != nil {
		return protocol.MakeErrReply("illegal commit index: " + string(snapshot[3]))
	}
	nodes, err := unmarshalNodes(snapshot[5:])
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	raft.selfNodeID = selfNodeId
	raft.state = state
	raft.leaderId = leaderId
	raft.term = term
	raft.committedIndex = commitIndex
	raft.proposedIndex = commitIndex
	raft.initLog(term, commitIndex, nil)
	raft.slots = make([]*Slot, slotCount)
	for _, node := range nodes {
		for _, slot := range node.Slots {
			raft.slots[int(slot.ID)] = slot
		}
		if node.getState() == leader {
			raft.leaderId = node.ID
		}
	}
	raft.nodes = nodes
	return nil
}
