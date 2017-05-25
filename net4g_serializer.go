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

type Serializer interface {
	RegisterById(t reflect.Type, id_at_most_one ...int) (id int, err error)
	RegisterByKey(t reflect.Type, key_at_most_one ...string) (key string, err error)
	Serialize(v interface{}) (data []byte, err error)
	Deserialize(data []byte) (v interface{}, err error)
}

type emptySerializer struct {
	ids        map[reflect.Type]int
	keys       map[reflect.Type]string
	typesOfId  map[int]reflect.Type
	typesOfKey map[string]reflect.Type
	count      int
	registered bool
	byId       bool
}

func (s *emptySerializer) RegisterById(t reflect.Type, id_at_most_one ...int) (id int, err error) {

	if t == nil || t.Kind() != reflect.Ptr {
		panic("type must be a pointer")
	}

	if len(s.keys) > 0 {
		panic("can not registered id and key in one serializer")
	}

	if len(id_at_most_one) > 1 {
		panic("only mapping one type with one id")
	}

	if _id, ok := s.ids[t]; ok {
		text := fmt.Sprintf("%s has been registered by %d", t.String(), _id)
		log4g.Error(text)
		return 0, errors.New(text)
	}

	if len(id_at_most_one) == 1 {
		id = id_at_most_one[0]
	} else {
		s.count++
		id = s.count
	}

	s.ids[t] = id
	s.typesOfId[id] = t

	s.byId = true
	s.registered = true

	return
}

func (s *emptySerializer) RegisterByKey(t reflect.Type, key_at_most_one ...string) (key string, err error) {

	if t == nil || t.Kind() != reflect.Ptr {
		panic("type must be a pointer")
	}

	if len(s.ids) > 0 {
		panic("can not registered key and id in one serializer")
	}

	if len(key_at_most_one) > 1 {
		panic("only mapping one type with one key")
	}

	if _key, ok := s.keys[t]; ok {
		text := fmt.Sprintf("%s has been registered by %s", t.Elem().Name(), _key)
		log4g.Error(text)
		err = errors.New(text)
		return
	}

	if len(key_at_most_one) == 1 {
		key = key_at_most_one[0]
	} else {
		key = t.String()
	}

	s.keys[t] = key
	s.typesOfKey[key] = t

	s.byId = false
	s.registered = true

	log4g.Info("%v register by key '%s'\n", t, key)

	return
}

func (s *emptySerializer) Serialize(v interface{}) (data []byte, err error) {
	return v.([]byte), nil
}

func (s *emptySerializer) Deserialize(data []byte) (v interface{}, err error) {
	return data, nil
}

func NewEmptySerializer() Serializer {
	return newEmptySerializer()
}

func newEmptySerializer() *emptySerializer {
	s := new(emptySerializer)
	s.ids = make(map[reflect.Type]int)
	s.typesOfId = make(map[int]reflect.Type)
	s.keys = make(map[reflect.Type]string)
	s.typesOfKey = make(map[string]reflect.Type)
	return s
}

type stringSerializer struct {
	Serializer
}

func (s *stringSerializer) Serialize(v interface{}) (data []byte, err error) {
	return []byte(v.(string)), nil
}

func (s *stringSerializer) Deserialize(data []byte) (v interface{}, err error) {
	return string(data), nil
}

func NewStringSerializer() Serializer {
	s := new(stringSerializer)
	s.Serializer = newEmptySerializer()
	return s
}

type jsonSerializer struct {
	*emptySerializer
}

func (s *jsonSerializer) Serialize(v interface{}) (data []byte, err error) {

	if !s.registered {
		panic("not registered any id or key")
	}

	t := reflect.TypeOf(v)

	if s.byId {
		if id, ok := s.ids[t]; ok {
			data, err = json.Marshal(v)
			if err != nil {
				log4g.Error(err)
				return
			}
			data = util.AddIntHeader(data, NetConfig.ProtobufIdSize, uint64(id), NetConfig.LittleEndian)
			if log4g.IsTrace() {
				log4g.Trace("serialized %v - %s", t, string(data))
			}
		} else {
			err = errors.New(fmt.Sprintf("%v is not registed by any id", t))
			log4g.Error(err)
		}
	} else {
		if key, ok := s.keys[t]; ok {
			m := map[string]interface{}{key: v}
			data, err = json.Marshal(m)
			if err != nil {
				log4g.Error(err)
				return
			}
			if log4g.IsTrace() {
				log4g.Trace("serialized %v - %s", t, string(data))
			}
		} else {
			log4g.Panic("%v is not registered by any key", t)
		}
	}

	return
}

func (s *jsonSerializer) Deserialize(data []byte) (v interface{}, err error) {

	if !s.registered {
		panic("not registered any id or key")
	}

	if s.byId {
		id := int(util.GetIntHeader(data, NetConfig.ProtobufIdSize, NetConfig.LittleEndian))
		if t, ok := s.typesOfId[id]; ok {
			value := reflect.New(t.Elem()).Interface()
			err = json.Unmarshal(data[NetConfig.ProtobufIdSize:], value)
			if err != nil {
				log4g.Error(err)
			} else {
				v = value
				log4g.Trace("rcvd %v - %s", t, string(data))
			}
		} else {
			err = errors.New(fmt.Sprintf("id[%d] is not registered by any type", id))
			log4g.Error(err)
		}
	} else {
		var m_raw map[string]json.RawMessage
		err = json.Unmarshal(data, &m_raw)
		if err != nil {
			log4g.Error(err)
			return nil, err
		}
		if len(m_raw) == 0 {
			text := fmt.Sprintf("invalid json: %v", string(data))
			log4g.Error(text)
			err = errors.New(text)
			return
		}
		for key, raw := range m_raw {
			if t, ok := s.typesOfKey[key]; ok {
				value := reflect.New(t.Elem()).Interface()
				err = json.Unmarshal(raw, value)
				if err != nil {
					log4g.Error(err)
				} else {
					v = value
					log4g.Trace("rcvd %v - %s", t, string(raw))
				}
			} else {
				err = errors.New(fmt.Sprintf("key '%s' is not registered by any type", key))
				log4g.Error(err)
			}
		}
	}
	return
}

func NewJsonSerializer() Serializer {
	s := new(jsonSerializer)
	s.emptySerializer = newEmptySerializer()
	return s
}

type protobufSerializer struct {
	*emptySerializer
}

func (s *protobufSerializer) Serialize(v interface{}) (data []byte, err error) {

	if !s.registered {
		log4g.Panic("not registered any id")
	}

	t := reflect.TypeOf(v)
	if id, ok := s.ids[t]; ok {
		data, err = proto.Marshal(v.(proto.Message))
		if err != nil {
			log4g.Error(err)
			return
		}
		data = util.AddIntHeader(data, NetConfig.ProtobufIdSize, uint64(id), NetConfig.LittleEndian)
	} else {
		err = errors.New(fmt.Sprintf("%v is not registed by any id", t))
	}
	return
}

func (s *protobufSerializer) Deserialize(data []byte) (v interface{}, err error) {
	if !s.registered {
		log4g.Panic("not registered any id")
	}
	id := int(util.GetIntHeader(data, NetConfig.ProtobufIdSize, NetConfig.LittleEndian))
	if t, ok := s.typesOfId[id]; ok {
		value := reflect.New(t.Elem()).Interface()
		err = proto.UnmarshalMerge(data, value.(proto.Message))
		if err != nil {
			log4g.Error(err)
		} else {
			v = value
			bytes, _ := json.Marshal(v)
			log4g.Trace("rcvd %v - %v", t, string(bytes))
		}
	} else {
		err = errors.New(fmt.Sprintf("id[%d] is not registered by any type", id))
		log4g.Error(err)
	}
	return
}

func NewProtobufSerializer() Serializer {
	s := new(protobufSerializer)
	s.emptySerializer = newEmptySerializer()
	return s
}
