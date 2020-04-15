/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/Common.h"
#include "common/OperationKeyUtils.h"
#include "storage/StorageFlags.h"
#include "storage/admin/RebuildIndexTask.h"

namespace nebula {
namespace storage {

ErrorOr<cpp2::ErrorCode, std::vector<AdminSubTask>>
RebuildIndexTask::genSubTasks() {
    std::vector<AdminSubTask> tasks;
    CHECK_NOTNULL(env_->kvstore_);
    auto space = ctx_.spaceId_;
    auto parts = ctx_.parts_;
    auto parameters = ctx_.parameters_;
    auto indexID = std::stoi(parameters.task_specfic_paras[0]);
    auto isOffline = strcasecmp("offline", parameters.task_specfic_paras[1].c_str());

    auto itemRet = getIndex(space, indexID);
    if (!itemRet.ok()) {
        LOG(ERROR) << "Index Not Found";
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }

    if (env_->rebuildIndexID_ != -1) {
        LOG(ERROR) << "Some index is rebuilding";
        return cpp2::ErrorCode::E_REBUILD_INDEX_FAILED;
    }

    env_->rebuildIndexID_ = indexID;
    auto item = itemRet.value();
    auto schemaID = item->get_schema_id();
    if (schemaID.getType() == nebula::meta::cpp2::SchemaID::Type::tag_id) {
        env_->rebuildTagID_ = schemaID.get_tag_id();
    } else {
        env_->rebuildEdgeType_ = schemaID.get_edge_type();
    }

    if (isOffline) {
        LOG(INFO) << "Rebuild Index Offline Space: " << space
                  << " Index: " << indexID;
    } else {
        LOG(INFO) << "Rebuild Index Space: " << space
                  << " Index: " << indexID;
    }

    for (PartitionID part : parts) {
        env_->rebuildPartID_->insert(std::make_pair(part, true));
        std::function<kvstore::ResultCode()> func = std::bind(&RebuildIndexTask::genSubTask,
                                                              this, space, part, schemaID,
                                                              indexID, item, isOffline);
        tasks.emplace_back(func, part);
    }
    return tasks;
}

kvstore::ResultCode RebuildIndexTask::genSubTask(GraphSpaceID space,
                                                 PartitionID part,
                                                 meta::cpp2::SchemaID schemaID,
                                                 int32_t indexID,
                                                 std::shared_ptr<meta::cpp2::IndexItem> item,
                                                 bool isOffline) {
    auto result = buildIndexGlobal(space, part, schemaID, indexID, item->get_fields());
    if (!result) {
        LOG(ERROR) << "Building index failed";
        return kvstore::ResultCode::E_BUILD_INDEX_FAILED;
    }

    if (!isOffline) {
        // Processing the operation logs
        result = buildIndexOnOperations(space, part);
        if (!result) {
            LOG(ERROR) << "Building index with operation logs failed";
            return kvstore::ResultCode::E_INVALID_OPERATION;
        }
    }
    return result;
}

kvstore::ResultCode RebuildIndexTask::buildIndexOnOperations(GraphSpaceID space,
                                                             PartitionID part) {
    int32_t lastProcessedOperationsNum = -1;
    std::vector<kvstore::KV> appendedIndex;
    appendedIndex.reserve(FLAGS_rebuild_index_batch_num);
    std::vector<std::string> removeKeys;
    removeKeys.reserve(FLAGS_rebuild_index_batch_num * 2);

    while (true) {
        if (canceled_) {
            LOG(ERROR) << "Rebuild index canceled";
            return kvstore::ResultCode::SUCCEEDED;
        }

        std::unique_ptr<kvstore::KVIterator> operationIter;
        auto operationPrefix = OperationKeyUtils::operationPrefix(part);
        auto operationRet = env_->kvstore_->prefix(space,
                                                   part,
                                                   operationPrefix,
                                                   &operationIter);
        if (operationRet != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Processing Part " << part << " Failed";
            return operationRet;
        }

        while (operationIter && operationIter->valid()) {
            lastProcessedOperationsNum += 1;
            auto opKey = operationIter->key();
            // auto opVal = iter->val();
            // replay operation record
            if (OperationKeyUtils::isModifyOperation(opKey)) {
                auto key = OperationKeyUtils::getOperationKey(opKey);
                // std::vector<kvstore::KV> data = {std::make_pair(std::move(key), "")};
                // appendedIndex.emplace_back(std::move(data));
            } else if (OperationKeyUtils::isDeleteOperation(opKey)) {
                auto opVal = operationIter->val();
                removeKeys.emplace_back(std::move(opVal));
            } else {
                LOG(ERROR) << "Unknow Operation Type";
                return kvstore::ResultCode::E_INVALID_OPERATION;
            }
            removeKeys.emplace_back(std::move(opKey));

            if (lastProcessedOperationsNum % FLAGS_rebuild_index_block_limit == 0) {
                folly::Baton<true, std::atomic> removeBaton;
                auto ret = processModifyOperation(space, part,
                                                  appendedIndex,
                                                  env_->kvstore_);
                if (kvstore::ResultCode::SUCCEEDED != ret) {
                    return ret;
                }

                ret = processRemoveOperation(space, part,
                                             removeKeys,
                                             env_->kvstore_);
                if (kvstore::ResultCode::SUCCEEDED != ret) {
                    return ret;
                }

                removeKeys.reserve(FLAGS_rebuild_index_batch_num * 2);
                appendedIndex.reserve(FLAGS_rebuild_index_batch_num);
            }
        }
    }

    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode
RebuildIndexTask::processModifyOperation(GraphSpaceID space,
                                         PartitionID part,
                                         std::vector<kvstore::KV>& data,
                                         kvstore::KVStore* kvstore) {
    folly::Baton<true, std::atomic> baton;
    kvstore::ResultCode result = kvstore::ResultCode::SUCCEEDED;
    kvstore->asyncMultiPut(space, part, std::move(data),
                           [&result, &baton](kvstore::ResultCode code) {
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Modify the index data failed";
            result = code;
        }
        baton.post();
    });
    baton.wait();
    return result;
}

kvstore::ResultCode
RebuildIndexTask::processRemoveOperation(GraphSpaceID space,
                                         PartitionID part,
                                         std::vector<std::string>& keys,
                                         kvstore::KVStore* kvstore) {
    folly::Baton<true, std::atomic> baton;
    kvstore::ResultCode result = kvstore::ResultCode::SUCCEEDED;
    kvstore->asyncMultiRemove(space, part, std::move(keys),
                              [&result, &baton](kvstore::ResultCode code) {
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Remove the operation log failed";
            result = code;
        }
        baton.post();
    });
    baton.wait();
    return result;
}

StatusOr<IndexValues>
RebuildIndexTask::collectIndexValues(RowReader* reader,
                                     const std::vector<nebula::meta::cpp2::ColumnDef>& cols) {
    IndexValues values;
    if (reader == nullptr) {
        return values;
    }
    for (auto& col : cols) {
        auto value = reader->getValueByName(col.get_name());
        auto encodeValue = IndexKeyUtils::encodeValue(std::move(value));
        values.emplace_back(value.type(), std::move(encodeValue));
    }
    return values;
}

}  // namespace storage
}  // namespace nebula
