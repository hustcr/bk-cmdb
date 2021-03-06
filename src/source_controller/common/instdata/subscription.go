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
 
package instdata

import (
	"configcenter/src/scene_server/event_server/types"
)

func DelSubscriptionByCondition(condition interface{}) error {
	err := DataH.DelByCondition(types.TableNameSubscription, condition)
	if nil != err {
		return err
	}
	return nil
}

func UpdateSubscriptionByCondition(data interface{}, condition interface{}) error {
	err := DataH.UpdateByCondition(types.TableNameSubscription, data, condition)
	if nil != err {
		return err
	}
	return nil
}

func GetOneSubscriptionByCondition(condition, result interface{}) error {
	return DataH.GetOneByCondition(types.TableNameSubscription, nil, condition, result)
}
func GetSubscriptionByCondition(fields []string, condition, result interface{}, sort string, skip, limit int) error {
	return DataH.GetMutilByCondition(types.TableNameSubscription, fields, condition, result, sort, skip, limit)
}
func GetSubscriptionCntByCondition(condition interface{}) (int, error) {
	cnt, err := DataH.GetCntByCondition(types.TableNameSubscription, condition)
	if nil != err {
		return 0, err
	}
	return cnt, nil
}

func CreateSubscription(input *types.Subscription) (int64, error) {
	id, err := DataH.GetIncID(types.TableNameSubscription)
	if err != nil {
		return 0, err
	}
	input.SubscriptionID = id
	DataH.Insert(types.TableNameSubscription, input)
	return id, nil
}
