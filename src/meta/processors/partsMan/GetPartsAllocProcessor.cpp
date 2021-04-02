/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/GetPartsAllocProcessor.h"

namespace nebula {
namespace meta {

void GetPartsAllocProcessor::process(const cpp2::GetPartsAllocReq& req) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
  auto spaceId = req.get_space_id();
  auto prefix = MetaServiceUtils::partPrefix(spaceId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (ret != kvstore::ResultCode::SUCCEEDED) {
    LOG(ERROR) << "Get parts failed";
    handleErrorCode(MetaCommon::to(ret));
    onFinished();
    return;
  }
  std::unordered_map<int, std::vector<nebula::HostAddr>> parts;
  while (iter->valid()) {
    auto key = iter->key();
    PartitionID partId;
    memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
    std::vector<HostAddr> partHosts = MetaServiceUtils::parsePartVal(iter->val());
    parts.emplace(partId, std::move(partHosts));
    iter->next();
  }
  handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
  resp_.set_parts(std::move(parts));
  onFinished();
}

}  // namespace meta
}  // namespace nebula
