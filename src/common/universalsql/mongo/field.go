/*
 * Tencent is pleased to support the open source community by making 蓝鲸 available.
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mongo

import (
	"encoding/json"

	"configcenter/src/common/mapstr"
	"configcenter/src/common/universalsql"
)

type FieldItem struct {
	Key    string
	Val    mapstr.MapStr
	parent *FieldItem
}

type LastItem struct {
	Key string
	Val interface{}
}

func (k *FieldItem) ToSQL() (string, error) {
	sql, err := json.Marshal(*k)
	return string(sql), err
}

func (l *LastItem) ToSQL() (string, error) {
	sql, err := json.Marshal(*l)
	return string(sql), err
}

func legal(key string) bool {
	if 0 == len(key) {
		return false
	}
	//TODO:any other illegal case

	return true
}

func (k *FieldItem) ToMapStr() mapstr.MapStr {
	rst := mapstr.New()

	if !legal(k.Key) {
		//drop it
		return rst
	}

	rst[k.Key] = k.Val
	return rst
}

func (l *LastItem) ToMapStr() mapstr.MapStr {
	rst := mapstr.New()

	if !legal(l.Key) {
		//drop it
		return rst
	}

	rst[l.Key] = l.Val
	return rst
}

//Comparision operator start

//Field create a new field
func Field(k string) *FieldItem {
	//check legality for k in func (k *FieldItem) ToMapStr()
	//if k is illegal, the field should be throw away
	return &FieldItem{Key: k, Val: mapstr.New(), parent: nil}
}

//Eq add an element like { <field> : { $eq: <val> } } for the field
func (k *FieldItem) Eq(val interface{}) *FieldItem {
	k.Val[universalsql.EQ] = val
	return k
}

//Neq add an element like { <field> : { $neq: <val> } } for the field
func (k *FieldItem) Neq(val interface{}) *FieldItem {
	k.Val[universalsql.NEQ] = val
	return k
}

//Gt add an element like { <field>: { $gt: <val> } } for the field
func (k *FieldItem) Gt(val interface{}) *FieldItem {
	k.Val[universalsql.GT] = val
	return k
}

//Gte add an element like { <field>: { $gte: <val> } } for the field
func (k *FieldItem) Gte(val interface{}) *FieldItem {
	k.Val[universalsql.GTE] = val
	return k
}

//Lt add an element like { <field>: { $lt: <val> } } for the field
func (k *FieldItem) Lt(val interface{}) *FieldItem {
	k.Val[universalsql.LT] = val
	return k
}

//Lte add an element like { <field>: { $lte: <val> } } for the field
func (k *FieldItem) Lte(val interface{}) *FieldItem {
	k.Val[universalsql.LTE] = val
	return k
}

//In add an element like { <field>: { $in: [ <val1>, <val2>,...<valn> ] } } for the field
func (k *FieldItem) In(val interface{}) *FieldItem {
	k.Val[universalsql.IN] = val
	return k
}

//Nin add an element like { <field>: { $nin: [ <val1>, <val2>,...<valn> ] } } for the field
func (k *FieldItem) Nin(val interface{}) *FieldItem {
	k.Val[universalsql.NIN] = val
	return k
}

//Comparision operator end
//Elements operator start
//Exists add an element like { <field>: { $exists: bool } } for the field
func (k *FieldItem) Exists(val bool) *FieldItem {
	k.Val[universalsql.EXISTS] = val
	return k
}

func (k *FieldItem) Type(val interface{}) *FieldItem {
	//TODO:type is not safe
	return k
}

//Elements operator end
//Array operator start
//All add an element like { <field>: { $all: [ <value1> , <value2> ... ] } } for the field
//Func All will find array that contains both the elements value1 and value2 and so on,
// without regard to order or other elements in the array
func (k *FieldItem) All(val interface{}) *FieldItem {
	k.Val[universalsql.ALL] = val
	return k
}

//ArrayMatch specify equality condition on an array, use the query document { <field>: <value> }
// where <value> is the exact array to match, including the order of the elements.
// Special attention is needed here, once the function is called, the field will no longer be able to call
// other operator functions, and the operator function called before this will be invalid.
// The corresponding field only retains the result of the call to this function.
func (k *FieldItem) ArrayMatch(val interface{}) *LastItem {
	var l LastItem
	l.Key = k.Key
	l.Val = val
	return &l
}

//ElemMatch specify multiple criteria on the elements of an array such that at least one array element satisfies
// all the specified criteria
// Please note that ElemMatch an EndElemMatch must be paired
func (k *FieldItem) ElemMatch() *FieldItem {
	//TODO:too complicated
	elemField := Field(universalsql.ELEMMATCH)
	k.Val[universalsql.ELEMMATCH] = elemField
	elemField.parent = k
	return elemField
}

//EndElemMatch end the process of generating the array element match query SQL
// This function will only work after ElemMatch was called, or it will do nothing.
func (k *FieldItem) EndElemMatch() *FieldItem {
	if nil != k.parent {
		k.parent.Val[k.Key] = k.Val
		return k.parent.EndElemMatch()
	}
	return k
}

//Size add an element like { <field>: { $size: value } } for the field
//Size matches any array with the number of elements specified by the argument.
func (k *FieldItem) Size(val int) *FieldItem {
	k.Val[universalsql.SIZE] = val
	return k
}

//Array operator end
