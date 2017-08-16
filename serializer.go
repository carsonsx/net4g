package net4g

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/carsonsx/log4g"
	"github.com/carsonsx/net4g/util"
	"github.com/golang/protobuf/proto"
	"reflect"
)

func Serialize(serializer Serializer, v interface{}, h ...interface{}) (data []byte, err error) {
	var header interface{}
	if len(h) > 0 {
		header = h[0]
	}
	data, err = serializer.Serialize(v, header)
	return
}

func NewRawPack(id interface{}, data ...[]byte) *RawPack {
	rp := new(RawPack)
	rp.Id = id
	if len(data) > 0 {
		rp.Data = data[0]
	}
	return rp
}

func NewRawPackByType(v interface{}, data ...[]byte) *RawPack {
	rp := new(RawPack)
	rp.Type = reflect.TypeOf(v)
	if len(data) > 0 {
		rp.Data = data[0]
	}
	return rp
}

type RawPack struct {
	Id   interface{}
	Type reflect.Type
	Data []byte
}

func (rp *RawPack) IntId() int {
	if rp.Id == nil {
		return 0
	}
	return rp.Id.(int)
}

func (rp *RawPack) StringId() string {
	if rp.Id == nil {
		return ""
	}
	return rp.Id.(string)
}

type Serializer interface {
	SetIdInitValue(id int)
	RegisterId(v interface{}, one_id ...interface{}) (id interface{}, err error)
	RegisterSerializeId(v interface{}, one_id ...interface{}) (id interface{}, err error)
	RegisterDeserializeId(v interface{}, one_id ...interface{}) (id interface{}, err error)
	Serialize(v, h interface{}) (data []byte, err error)
	Deserialize(raw []byte) (v, h interface{}, rp *RawPack, err error)
	RangeId(f func(id interface{}, t reflect.Type))
}

func NewEmptySerializer() *EmptySerializer {
	s := new(EmptySerializer)
	s.SerializerTypeIdMap = make(map[reflect.Type]interface{})
	s.SerializerIdTypeMap = make(map[interface{}]reflect.Type)
	s.DeserializerTypeIdMap = make(map[reflect.Type]interface{})
	s.DeserializerIdTypeMap = make(map[interface{}]reflect.Type)
	s.id = 1
	return s
}

type EmptySerializer struct {
	SerializerTypeIdMap   map[reflect.Type]interface{}
	SerializerIdTypeMap   map[interface{}]reflect.Type
	DeserializerTypeIdMap map[reflect.Type]interface{}
	DeserializerIdTypeMap map[interface{}]reflect.Type
	ids                   []interface{}
	id                    int
}

type registerType int

const (
	register_serialize registerType = iota
	register_deserialize
	register_both
)

func (s *EmptySerializer) SetIdInitValue(id int) {
	s.id = id
}

func (s *EmptySerializer) RegisterId(v interface{}, one_id ...interface{}) (id interface{}, err error) {
	return s.register(register_both, v, one_id...)
}

func (s *EmptySerializer) RegisterSerializeId(v interface{}, one_id ...interface{}) (id interface{}, err error) {
	return s.register(register_serialize, v, one_id...)
}

func (s *EmptySerializer) RegisterDeserializeId(v interface{}, one_id ...interface{}) (id interface{}, err error) {
	return s.register(register_deserialize, v, one_id...)
}

func (s *EmptySerializer) register(rt registerType, v interface{}, one_id ...interface{}) (id interface{}, err error) {

	t := reflect.TypeOf(v)
	if t == nil || t.Kind() != reflect.Ptr {
		panic("interface type must be a pointer")
	}

	if len(one_id) > 1 {
		panic("one id one type")
	}

	if len(one_id) == 1 {
		id = one_id[0]
	} else {
		id = s.id
		s.id++
	}

	// register for serialize and deserialize

	if rt == register_both || rt == register_serialize {
		s.SerializerTypeIdMap[t] = id
		s.SerializerIdTypeMap[id] = t
		s.ids = append(s.ids, id)
		log4g.Info("registered   serialize mapping relations between id[%v] and type[%v]", id, t)
	}
	if rt == register_both || rt == register_deserialize {
		s.DeserializerTypeIdMap[t] = id
		s.DeserializerIdTypeMap[id] = t
		s.ids = append(s.ids, id)
		log4g.Info("registered deserialize mapping relations between id[%v] and type[%v]", id, t)
	}

	return
}

func (s *EmptySerializer) RangeId(f func(id interface{}, t reflect.Type)) {
	for _, id := range s.ids {
		f(id, s.SerializerIdTypeMap[id])
	}
}

func (s *EmptySerializer) RangeIdInt(f func(id int, t reflect.Type)) {
	for _, id := range s.ids {
		f(id.(int), s.SerializerIdTypeMap[id])
	}
}

func (s *EmptySerializer) RangeIdString(f func(id int, t reflect.Type)) {
	for _, id := range s.ids {
		f(id.(int), s.SerializerIdTypeMap[id])
	}
}

func (s *EmptySerializer) PreRawPack(rp *RawPack) {
	if rp.Type == nil {
		if rp.Id != nil {
			rp.Type = s.SerializerIdTypeMap[rp.Id]
		}
	}
	if rp.Id == nil && rp.Type != nil {
		if len(s.SerializerTypeIdMap) > 0 {
			rp.Id = s.SerializerTypeIdMap[rp.Type]
		}
	}
}

func NewByteSerializer() Serializer {
	s := new(ByteSerializer)
	s.EmptySerializer = NewEmptySerializer()
	return s
}

type ByteSerializer struct {
	*EmptySerializer
}

func (s *ByteSerializer) Serialize(v, h interface{}) (data []byte, err error) {
	if rp, ok := v.(*RawPack); ok {
		s.PreRawPack(rp)
		log4g.Trace("serialized - %v", rp)
		data = util.AddIntHeader(rp.Data, NetConfig.IdSize, uint64(rp.IntId()), NetConfig.LittleEndian)
	} else {
		data = v.([]byte)
	}
	if NetConfig.HeaderSize > NetConfig.IdSize {
		if h == nil {
			log4g.Panic("header cannot be nil")
		}
		header := h.([]byte)
		if NetConfig.HeaderSize != len(header)+NetConfig.IdSize {
			log4g.Panic("invalid header length: excepted %d, actual %d", NetConfig.HeaderSize-NetConfig.IdSize, len(header))
		}
		_data := data
		data = make([]byte, len(header)+len(_data))
		copy(data, header)
		copy(data[len(header):], _data)
	}
	return
}

func (s *ByteSerializer) Deserialize(raw []byte) (v, h interface{}, rp *RawPack, err error) {
	rp = new(RawPack)
	rp.Id = int(util.GetIntHeader(raw, NetConfig.IdSize, NetConfig.LittleEndian))
	rp.Data = raw[NetConfig.IdSize:]
	v = rp.Data
	log4g.Trace("deserialize - %v", *rp)
	return
}

func NewStringSerializer() Serializer {
	s := new(StringSerializer)
	s.EmptySerializer = NewEmptySerializer()
	return s
}

type StringSerializer struct {
	*EmptySerializer
}

func (s *StringSerializer) Serialize(v, h interface{}) (raw []byte, err error) {
	return []byte(v.(string)), nil
}

func (s *StringSerializer) Deserialize(raw []byte) (v, h interface{}, rp *RawPack, err error) {
	rp = new(RawPack)
	return string(raw), nil, rp, nil
}

func NewJsonSerializer() Serializer {
	s := new(JsonSerializer)
	s.EmptySerializer = NewEmptySerializer()
	return s
}

type JsonSerializer struct {
	*EmptySerializer
}

func (s *JsonSerializer) Serialize(v, h interface{}) (data []byte, err error) {

	if rp, ok := v.(*RawPack); ok {
		s.PreRawPack(rp)
		if id, ok := s.SerializerTypeIdMap[rp.Type]; ok {
			if intId, ok := id.(int); ok {
				data = util.AddIntHeader(rp.Data, NetConfig.IdSize, uint64(intId), NetConfig.LittleEndian)
			} else if strId, ok := id.(string); ok {
				m := map[string]json.RawMessage{strId: rp.Data}
				data, err = json.Marshal(m)
				if err != nil {
					log4g.Error(err)
					return
				}
				if log4g.IsTraceEnabled() {
					log4g.Trace("serialized %v - %s", rp.Type, string(data))
				}
			}

		} else {
			err = errors.New(fmt.Sprintf("%v is not registed by any id", rp.Type))
			log4g.Error(err)
		}
	} else {
		t := reflect.TypeOf(v)
		if t == nil || t.Kind() != reflect.Ptr {
			panic("value type must be a pointer")
		}
		if id, ok := s.SerializerTypeIdMap[t]; ok {
			if intId, ok := id.(int); ok {
				data, err = json.Marshal(v)
				if err != nil {
					log4g.Error(err)
					return
				}
				data = util.AddIntHeader(data, NetConfig.IdSize, uint64(intId), NetConfig.LittleEndian)
			} else if strId, ok := id.(string); ok {
				m := map[string]interface{}{strId: v}
				data, err = json.Marshal(m)
				if err != nil {
					log4g.Error(err)
					return
				}
				if log4g.IsTraceEnabled() {
					log4g.Trace("serialized %v - %s", t, string(data))
				}
			}
			if log4g.IsTraceEnabled() {
				log4g.Trace("serialized %v - %s", t, string(data))
			}
		} else {
			err = errors.New(fmt.Sprintf("%v is not registed by any id", t))
			log4g.Error(err)
			log4g.Info("serialize ids: %v", s.SerializerIdTypeMap)
		}
	}

	return
}

func (s *JsonSerializer) Deserialize(raw []byte) (v, h interface{}, rp *RawPack, err error) {

	rp = new(RawPack)
	rp.Data = raw

	if len(s.ids) == 0 {
		return
	}

	if _, ok := s.ids[0].(int); ok {
		if len(raw) < NetConfig.IdSize {
			text := fmt.Sprintf("message length [%d] is short than id size [%d]", len(raw), NetConfig.IdSize)
			err = errors.New(text)
			log4g.Error(err)
			return
		}

		rp.Id = int(util.GetIntHeader(raw, NetConfig.IdSize, NetConfig.LittleEndian))
		rp.Data = raw[NetConfig.IdSize:]
		var ok bool
		if rp.Type, ok = s.DeserializerIdTypeMap[rp.Id]; ok {
			v = reflect.New(rp.Type.Elem()).Interface()
			if len(rp.Data) == 0 {
				return
			}
			err = json.Unmarshal(rp.Data, v)
			if err != nil {
				log4g.Error(err)
			} else {
				log4g.Trace("deserialize %v - %s", rp.Type, string(rp.Data))
			}
		} else {
			err = errors.New(fmt.Sprintf("id[%v] is not registered by any type", rp.Id))
			log4g.Error(err)
			log4g.Info("registered ids: %v", s.DeserializerIdTypeMap)
		}
	} else if _, ok := s.ids[0].(string); ok {
		var m_raw map[string]json.RawMessage
		err = json.Unmarshal(raw, &m_raw)
		if err != nil {
			log4g.Error(err)
			return
		}
		if len(m_raw) == 0 {
			text := fmt.Sprintf("invalid json: %v", string(raw))
			err = errors.New(text)
			log4g.Error(err)
			return
		}
		for rp.Id, rp.Data = range m_raw {
			var ok bool
			if rp.Type, ok = s.DeserializerIdTypeMap[rp.Id]; ok {
				v = reflect.New(rp.Type.Elem()).Interface()
				if len(rp.Data) == 0 {
					continue
				}
				err = json.Unmarshal(rp.Data, v)
				if err != nil {
					log4g.Error(err)
				} else {
					log4g.Trace("deserialize %v - %s", rp.Type, string(raw))
					break
				}
			} else {
				err = errors.New(fmt.Sprintf("id[%s] is not registered by any type", rp.Id))
				log4g.Error(err)
				log4g.Debug(s.DeserializerIdTypeMap)
			}
		}
	}
	return
}

func NewProtobufSerializer() Serializer {
	s := new(ProtobufSerializer)
	s.EmptySerializer = NewEmptySerializer()
	return s
}

type ProtobufSerializer struct {
	*EmptySerializer
}

func (s *ProtobufSerializer) Serialize(v, h interface{}) (data []byte, err error) {

	if rp, ok := v.(*RawPack); ok {
		s.PreRawPack(rp)
		data = util.AddIntHeader(rp.Data, NetConfig.IdSize, uint64(rp.IntId()), NetConfig.LittleEndian)
		if log4g.IsDebugEnabled() {
			bytes, _ := json.Marshal(v)
			log4g.Trace("serialize %d - %v", rp.Id, string(bytes))
		}
	} else {
		t := reflect.TypeOf(v)
		if t == nil || t.Kind() != reflect.Ptr {
			panic("value type must be a pointer")
		}
		if id, ok := s.SerializerTypeIdMap[t]; ok {
			data, err = proto.Marshal(v.(proto.Message))
			if err != nil {
				log4g.Error(err)
				return
			}
			data = util.AddIntHeader(data, NetConfig.IdSize, uint64(id.(int)), NetConfig.LittleEndian)
			if log4g.IsDebugEnabled() {
				bytes, _ := json.Marshal(v)
				log4g.Trace("serialize %v - %v", t, string(bytes))
			}
		} else {
			err = errors.New(fmt.Sprintf("%v is not registed by any id", t))
		}
	}

	return
}

func (s *ProtobufSerializer) Deserialize(raw []byte) (v, h interface{}, rp *RawPack, err error) {

	if len(raw) < NetConfig.IdSize {
		text := fmt.Sprintf("message length [%d] is short than id size [%d]", len(raw), NetConfig.IdSize)
		err = errors.New(text)
		log4g.Error(err)
		return
	}

	rp = new(RawPack)
	rp.Id = int(util.GetIntHeader(raw, NetConfig.IdSize, NetConfig.LittleEndian))
	rp.Data = raw[NetConfig.IdSize:]
	var ok bool
	if rp.Type, ok = s.DeserializerIdTypeMap[rp.Id]; ok {
		v = reflect.New(rp.Type.Elem()).Interface()
		if len(rp.Data) == 0 {
			return
		}
		err = proto.UnmarshalMerge(rp.Data, v.(proto.Message))
		if err != nil {
			log4g.Error(err)
		} else {
			if log4g.IsDebugEnabled() {
				bytes, _ := json.Marshal(v)
				log4g.Trace("deserialize %v - %v", rp.Type, string(bytes))
			}
		}
	}
	return
}
