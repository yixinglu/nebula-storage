/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "fs/TempDir.h"
#include <gtest/gtest.h>
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/admin/AdminTaskManager.h"
#include "storage/admin/RebuildTagIndexTask.h"
#include "storage/admin/RebuildEdgeIndexTask.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"

namespace nebula {
namespace storage {

TEST(RebuildIndexTest, RebuildTagIndexOffline) {
    fs::TempDir rootPath("/tmp/RebuildTagIndexOfflineTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    // Add Vertices
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    auto manager = AdminTaskManager::instance();
    manager->init();

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    request.set_job_id(1);
    request.set_task_id(1);
    request.set_para(std::move(parameter));
    request.set_concurrency(3);

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);

    auto task = std::make_shared<RebuildTagIndexTask>(env, std::move(context));
    manager->addAsyncTask(task);

    // Wait for the task finished
    while (!manager->isFinished(context.jobId_, context.taskId_)) {
        ::usleep(10);
    }

    // Check the result

    manager->shutdown();
}

TEST(RebuildIndexTest, RebuildTagIndexOnline) {
    fs::TempDir rootPath("/tmp/RebuildTagIndexOnlineTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    // Add Vertices
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    auto manager = AdminTaskManager::instance();
    manager->init();

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    request.set_job_id(1);
    request.set_task_id(1);
    request.set_para(std::move(parameter));
    request.set_concurrency(3);

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);

    auto task = std::make_shared<RebuildTagIndexTask>(env, std::move(context));
    manager->addAsyncTask(task);

    // Wait for the task finished
    while (!manager->isFinished(context.jobId_, context.taskId_)) {
        ::usleep(10);
    }

    // Check the result

    manager->shutdown();
}

TEST(RebuildIndexTest, RebuildEdgeIndexOffline) {
    fs::TempDir rootPath("/tmp/RebuildEdgeIndexOfflineTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    // Add Edges
    auto* processor = AddEdgesProcessor::instance(env, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    auto manager = AdminTaskManager::instance();
    manager->init();

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    request.set_job_id(2);
    request.set_task_id(1);
    request.set_para(std::move(parameter));
    request.set_concurrency(3);

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);
    auto task = std::make_shared<RebuildEdgeIndexTask>(env, std::move(context));
    manager->addAsyncTask(task);

    while (!manager->isFinished(context.jobId_, context.taskId_)) {
        ::usleep(10);
    }
    manager->shutdown();
}

TEST(RebuildIndexTest, RebuildEdgeIndexOnline) {
    fs::TempDir rootPath("/tmp/RebuildEdgeIndexOnlineTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    // Add Edges
    auto* processor = AddEdgesProcessor::instance(env, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    auto manager = AdminTaskManager::instance();
    manager->init();

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    request.set_job_id(2);
    request.set_task_id(1);
    request.set_para(std::move(parameter));
    request.set_concurrency(3);

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);
    auto task = std::make_shared<RebuildEdgeIndexTask>(env, std::move(context));
    manager->addAsyncTask(task);

    while (!manager->isFinished(context.jobId_, context.taskId_)) {
        ::usleep(10);
    }

    manager->shutdown();
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
