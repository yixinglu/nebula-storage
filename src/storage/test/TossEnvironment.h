/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "TossTestUtils.h"

#define FLOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

DECLARE_int32(heartbeat_interval_secs);
DEFINE_string(meta_server, "192.168.8.5:6500", "Meta servers' address.");

namespace nebula {
namespace storage {

const std::string kMetaName = "hp-server";  // NOLINT
constexpr int32_t kMetaPort = 6500;
const std::string kSpaceName = "test";   // NOLINT
constexpr int32_t kPart = 5;
constexpr int32_t kReplica = 3;
constexpr int32_t kSum = 10000 * 10000;

using StorageClient = storage::GraphStorageClient;
struct TossEnvironment {
    static TossEnvironment* getInstance(const std::string& metaName, int32_t metaPort) {
        static TossEnvironment inst(metaName, metaPort);
        return &inst;
    }

    TossEnvironment(const std::string& metaName, int32_t metaPort) {
        executor_ = std::make_shared<folly::IOThreadPoolExecutor>(20);
        mClient_ = setupMetaClient(metaName, metaPort);
        sClient_ = std::make_unique<StorageClient>(executor_, mClient_.get());
        interClient_ = std::make_unique<storage::InternalStorageClient>(executor_, mClient_.get());
        schemaMan_ = meta::SchemaManager::create(mClient_.get());
    }

    std::unique_ptr<meta::MetaClient>
    setupMetaClient(const std::string& metaName, uint32_t metaPort) {
        std::vector<HostAddr> metas;
        metas.emplace_back(HostAddr(metaName, metaPort));
        meta::MetaClientOptions options;
        auto client = std::make_unique<meta::MetaClient>(executor_, metas, options);
        if (!client->waitForMetadReady()) {
            LOG(FATAL) << "!client->waitForMetadReady()";
        }
        return client;
    }

    /*
     * setup space and edge type, then describe the cluster
     * */
    void init(const std::string& spaceName,
              size_t part,
              int replica,
              std::vector<meta::cpp2::ColumnDef>& colDefs) {
        if (spaceId_ != 0) {
            return;
        }
        spaceId_ = setupSpace(spaceName, part, replica);
        edgeType_ = setupEdgeSchema("test_edge", colDefs);

        int sleepSecs = FLAGS_heartbeat_interval_secs + 2;
        while (sleepSecs) {
            LOG(INFO) << "sleep for " << sleepSecs-- << " sec";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        auto stVIdLen = mClient_->getSpaceVidLen(spaceId_);
        LOG_IF(FATAL, !stVIdLen.ok());
        vIdLen_ = stVIdLen.value();

        bool leaderLoaded = false;
        while (!leaderLoaded) {
            auto statusOrLeaderMap = mClient_->loadLeader();
            if (!statusOrLeaderMap.ok()) {
                LOG(FATAL) << "mClient_->loadLeader() failed!!!!!!";
            }

            for (auto& leader : statusOrLeaderMap.value()) {
                LOG(INFO) << "spaceId=" << leader.first.first
                            << ", part=" << leader.first.second
                            << ", host=" << leader.second;
                if (leader.first.first == spaceId_) {
                    leaderLoaded = true;
                }
            }
        }
    }

    int32_t getSpaceId() { return spaceId_; }

    int setupSpace(const std::string& spaceName, int nPart, int nReplica) {
        auto fDropSpace = mClient_->dropSpace(spaceName, true);
        fDropSpace.wait();
        LOG(INFO) << "drop space " << spaceName;

        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name(spaceName);
        spaceDesc.set_partition_num(nPart);
        spaceDesc.set_replica_factor(nReplica);
        meta::cpp2::ColumnTypeDef colType;
        colType.set_type(meta::cpp2::PropertyType::INT64);
        spaceDesc.set_vid_type(colType);
        spaceDesc.set_isolation_level(meta::cpp2::IsolationLevel::TOSS);

        auto fCreateSpace = mClient_->createSpace(spaceDesc, true);
        fCreateSpace.wait();
        if (!fCreateSpace.valid()) {
            LOG(FATAL) << "!fCreateSpace.valid()";
        }
        if (!fCreateSpace.value().ok()) {
            LOG(FATAL) << "!fCreateSpace.value().ok(): "
                       << fCreateSpace.value().status().toString();
        }
        auto spaceId = fCreateSpace.value().value();
        LOG(INFO) << folly::sformat("spaceId = {}", spaceId);
        return spaceId;
    }

    EdgeType setupEdgeSchema(const std::string& edgeName,
                             std::vector<meta::cpp2::ColumnDef> columns) {
        meta::cpp2::Schema schema;
        schema.set_columns(std::move(columns));

        auto fCreateEdgeSchema = mClient_->createEdgeSchema(spaceId_, edgeName, schema, true);
        fCreateEdgeSchema.wait();

        if (!fCreateEdgeSchema.valid() || !fCreateEdgeSchema.value().ok()) {
            LOG(FATAL) << "createEdgeSchema failed";
        }
        return fCreateEdgeSchema.value().value();
    }

    cpp2::EdgeKey generateEdgeKey(int64_t srcId, int rank, int dstId = 0) {
        cpp2::EdgeKey edgeKey;
        edgeKey.set_src(srcId);

        edgeKey.set_edge_type(edgeType_);
        edgeKey.set_ranking(rank);
        if (dstId == 0) {
            dstId = kSum - srcId;
        }
        edgeKey.set_dst(dstId);
        return edgeKey;
    }

    cpp2::NewEdge generateEdge(int srcId,
                               int rank,
                               std::vector<nebula::Value> values,
                               int dstId = 0) {
        cpp2::NewEdge newEdge;

        cpp2::EdgeKey edgeKey = generateEdgeKey(srcId, rank, dstId);
        newEdge.set_key(std::move(edgeKey));
        newEdge.set_props(std::move(values));

        return newEdge;
    }

    cpp2::NewEdge reverseEdge(const cpp2::NewEdge& e0) {
        cpp2::NewEdge e(e0);
        std::swap(e.key.src, e.key.dst);
        e.key.edge_type *= -1;
        return e;
    }

    cpp2::ErrorCode syncAddMultiEdges(std::vector<cpp2::NewEdge>& edges, bool useToss) {
        bool retLeaderChange = false;
        int32_t retry = 0;
        int32_t retryMax = 10;
        do {
            if (retry > 0 && retry != retryMax) {
                LOG(INFO) << "\n leader changed, retry=" << retry;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                retLeaderChange = false;
            }
            auto f = addEdgesAsync(edges, useToss);
            f.wait();
            if (!f.valid()) {
                return cpp2::ErrorCode::E_UNKNOWN;
            }
            if (!f.value().succeeded()) {
                LOG(INFO) << "addEdgeAsync() !f.value().succeeded()";
            }

            std::vector<cpp2::ExecResponse>& execResps = f.value().responses();
            for (auto& execResp : execResps) {
                // ResponseCommon
                auto& respComn = execResp.result;
                auto& failedParts = respComn.get_failed_parts();
                for (auto& part : failedParts) {
                    if (part.code == cpp2::ErrorCode::E_LEADER_CHANGED) {
                        retLeaderChange = true;
                    }
                }
            }

            auto parts = f.value().failedParts();
            for (auto& part : parts) {
                if (part.second == cpp2::ErrorCode::E_LEADER_CHANGED) {
                    retLeaderChange = true;
                    LOG(INFO) << "E_LEADER_CHANGED retry";
                    break;
                } else {
                    auto icode = static_cast<int32_t>(part.second);
                    FLOG_FMT("failed: (space,part)=({},{}), code {}", spaceId_, part.first, icode);
                }
            }
            if (++retry == retryMax) {
                break;
            }
        } while (retLeaderChange);
        return cpp2::ErrorCode::SUCCEEDED;
    }

    cpp2::ErrorCode syncAddEdge(const cpp2::NewEdge& edge, bool useToss = true) {
        std::vector<cpp2::NewEdge> edges{edge};
        return syncAddMultiEdges(edges, useToss);
    }

    folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
    addEdgesAsync(const std::vector<cpp2::NewEdge>& edges, bool useToss = true) {
        auto propNames = makeColNames(edges.back().props.size());
        return sClient_->addEdges(spaceId_, edges, propNames, true, nullptr, useToss);
    }

    static std::vector<std::string> makeColNames(size_t n) {
        std::vector<std::string> colNames;
        for (auto i = 0U; i < n; ++i) {
            colNames.emplace_back(folly::sformat("c{}", i+1));
        }
        return colNames;
    }

    std::vector<Value> getProps(cpp2::NewEdge edge) {
        // nebula::DataSet ds;  ===> will crash if not set
        nebula::DataSet ds({kSrc, kType, kRank, kDst});
        std::vector<Value> edgeInfo{edge.key.src,
                                    edge.key.edge_type,
                                    edge.key.ranking,
                                    edge.key.dst};
        List row(std::move(edgeInfo));
        ds.rows.emplace_back(std::move(row));

        std::vector<cpp2::EdgeProp> props;
        cpp2::EdgeProp oneProp;
        oneProp.type = edge.key.edge_type;
        props.emplace_back(oneProp);

        auto frpc = sClient_->getProps(spaceId_,
                                       std::move(ds), /*DataSet*/
                                       nullptr, /*vector<cpp2::VertexProp>*/
                                       &props, /*vector<cpp2::EdgeProp>*/
                                       nullptr /*expressions*/).via(executor_.get());
        frpc.wait();
        if (!frpc.valid()) {
            LOG(FATAL) << "getProps rpc invalid()";
        }
        StorageRpcResponse<cpp2::GetPropResponse>& rpcResp = frpc.value();

        LOG(INFO) << "rpcResp.succeeded()=" << rpcResp.succeeded()
                  << ", responses().size()=" << rpcResp.responses().size()
                  << ", failedParts().size()=" << rpcResp.failedParts().size();

        auto resps = rpcResp.responses();
        if (resps.empty()) {
            LOG(FATAL) << "getProps() resps.empty())";
        }
        cpp2::GetPropResponse& propResp = resps.front();
        cpp2::ResponseCommon result = propResp.result;
        std::vector<cpp2::PartitionResult>& fparts = result.failed_parts;
        if (!fparts.empty()) {
            for (cpp2::PartitionResult& res : fparts) {
                LOG(INFO) << "part_id: " << res.part_id
                          << ", part leader " << res.leader
                          << ", code " << static_cast<int>(res.code);
            }
            LOG(FATAL) << "getProps() !failed_parts.empty())";
        }
        nebula::DataSet& dataSet = propResp.props;
        std::vector<Row>& rows = dataSet.rows;
        if (rows.empty()) {
            LOG(FATAL) << "getProps() dataSet.rows.empty())";
        }
        std::vector<Value>& values = rows[0].values;
        if (values.empty()) {
            LOG(FATAL) << "getProps() values.empty())";
        }
        return values;
    }

    folly::SemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>
    getNeighborsWrapper(const std::vector<cpp2::NewEdge>& edges,
                        int64_t limit = std::numeric_limits<int64_t>::max()) {
        // para3
        std::vector<Row> vertices;
        std::set<Value> vids;
        for (auto& e : edges) {
            vids.insert(e.key.src);
        }
        for (auto& vid : vids) {
            Row row;
            row.emplace_back(vid);
            vertices.emplace_back(row);
        }
        // para 4
        std::vector<EdgeType> edgeTypes;
        // para 5
        cpp2::EdgeDirection edgeDirection = cpp2::EdgeDirection::BOTH;
        // para 6
        std::vector<cpp2::StatProp>* statProps = nullptr;
        // para 7
        std::vector<cpp2::VertexProp>* vertexProps = nullptr;
        // para 8
        const std::vector<cpp2::EdgeProp> edgeProps;
        // para 9
        const std::vector<cpp2::Expr>* expressions = nullptr;
        // para 10
        bool dedup = false;
        // para 11
        bool random = false;
        // para 12
        const std::vector<cpp2::OrderBy> orderBy = std::vector<cpp2::OrderBy>();

        auto colNames = makeColNames(edges.back().props.size());

        return sClient_->getNeighbors(spaceId_,
                                      colNames,
                                      vertices,
                                      edgeTypes,
                                      edgeDirection,
                                      statProps,
                                      vertexProps,
                                      &edgeProps,
                                      expressions,
                                      dedup,
                                      random,
                                      orderBy,
                                      limit);
    }

    /**
     * @brief Get the Nei Props object,
     *        will unique same src & dst input edges.
     */
    std::vector<std::string> getNeiProps(const std::vector<cpp2::NewEdge>& _edges,
                                         int64_t limit = std::numeric_limits<int64_t>::max()) {
        bool retLeaderChange = false;
        auto edges(_edges);
        std::sort(edges.begin(), edges.end(), [](const auto& a, const auto& b) {
            if (a.key.src == b.key.src) {
                return a.key.dst < b.key.dst;
            }
            return a.key.src < b.key.src;
        });
        auto last = std::unique(edges.begin(), edges.end(), [](const auto& a, const auto& b) {
            return a.key.src == b.key.src && a.key.dst == b.key.dst;
        });
        edges.erase(last, edges.end());
        LOG(INFO) << "_edges.size()=" << _edges.size() << ", edges.size()=" << edges.size();
        do {
            auto f = getNeighborsWrapper(edges, limit);
            f.wait();
            if (!f.valid()) {
                LOG(ERROR) << "!f.valid()";
                break;
            }
            if (f.value().succeeded()) {
                return extractMultiRpcResp(f.value());
            } else {
                LOG(ERROR) << "!f.value().succeeded()";
            }
            auto parts = f.value().failedParts();
            for (auto& part : parts) {
                if (part.second == cpp2::ErrorCode::E_LEADER_CHANGED) {
                    retLeaderChange = true;
                    break;
                }
            }
        } while (retLeaderChange);
        LOG(ERROR) << "getOutNeighborsProps failed";
        std::vector<std::string> ret;
        return ret;
    }

    using RpcResp = StorageRpcResponse<cpp2::GetNeighborsResponse>;

    static std::vector<std::string> extractMultiRpcResp(RpcResp& rpc) {
        std::vector<std::string> ret;
        LOG(INFO) << "rpc.responses().size()=" << rpc.responses().size();
        for (auto& resp : rpc.responses()) {
            auto sub = extractNeiProps(resp);
            ret.insert(ret.end(), sub.begin(), sub.end());
        }
        return ret;
    }


    static std::vector<std::string> extractNeiProps(cpp2::GetNeighborsResponse& resp) {
        std::vector<std::string> ret;
        auto& ds = resp.vertices;
        LOG(INFO) << "ds.rows.size()=" << ds.rows.size();
        for (auto& row : ds.rows) {
            if (row.values.size() < 4) {
                continue;
            }
            LOG(INFO) << "row.values.size()=" << row.values.size();
            ret.emplace_back(row.values[3].toString());
        }
        return ret;
    }

    int32_t getCountOfNeighbors(const std::vector<cpp2::NewEdge>& edges) {
        int32_t ret = 0;
        auto neiPropsVec = getNeiProps(edges);
        for (auto& prop : neiPropsVec) {
            ret += countSquareBrackets(prop);
        }
        return ret;
    }

    static int32_t countSquareBrackets(const std::vector<std::string>& str) {
        int32_t ret = 0;
        for (auto& s : str) {
            ret += countSquareBrackets(s);
        }
        return ret;
    }

    static int32_t countSquareBrackets(const std::string& str) {
        auto ret = TossTestUtils::splitNeiResult(str);
        return ret.size();
    }

    static std::vector<std::string> extractProps(const std::string& props) {
        std::vector<std::string> ret;
        if (props.find('[') == std::string::npos) {
            return ret;
        }
        // props string shoule be [[...]], trim the []
        auto props1 = props.substr(2, props.size() - 4);
        folly::split("],[", props1, ret, true);
        std::sort(ret.begin(), ret.end(), [](const std::string& a, const std::string& b) {
            std::vector<std::string> svec1;
            std::vector<std::string> svec2;
            folly::split(",", a, svec1);
            folly::split(",", b, svec2);
            auto i1 = std::atoi(svec1[4].data());
            auto i2 = std::atoi(svec2[4].data());
            return i1 < i2;
        });
        return ret;
    }

    std::set<std::string> extractStrVals(const std::vector<std::string>& svec) {
        auto len = 36;
        std::set<std::string> strVals;
        for (auto& e : svec) {
            strVals.insert(e.substr(e.size() - len));
        }
        // std::sort(strVals.begin(), strVals.end());
        return strVals;
    }

    std::vector<std::string> diffProps(std::vector<std::string> actual,
                                       std::vector<std::string> expect) {
        std::sort(actual.begin(), actual.end());
        std::sort(expect.begin(), expect.end());
        std::vector<std::string> diff;

        std::set_difference(actual.begin(), actual.end(), expect.begin(), expect.end(),
                            std::inserter(diff, diff.begin()));
        return diff;
    }

    cpp2::NewEdge dupEdge(const cpp2::NewEdge& e) {
        cpp2::NewEdge dupEdge{e};
        int n = e.props[0].getInt() / 1024 + 1;
        dupEdge.props = TossTestUtils::genSingleVal(n);
        return dupEdge;
    }

    /**
     * @brief gen num edges base from src
     *        dst shoule begin from [src + 1, src + num + 1)
     * @param extraSameKey
     *        if set, gen another edge, has the same src & dst will always equal to src + 1
     */
    std::vector<cpp2::NewEdge> generateMultiEdges(int num, int64_t src, bool extraSameKey = false) {
        LOG_IF(FATAL, num <= 0) << "num must > 0";
        num += static_cast<int>(extraSameKey);
        auto vals = TossTestUtils::genValues(num);
        std::vector<cpp2::NewEdge> edges;
        for (int i = 0; i < num; ++i) {
            auto dst = extraSameKey ? 1 : src + i + 1;
            edges.emplace_back(generateEdge(src, 0, vals[i], dst));
        }
        return edges;
    }

    void insertMutliLocks(const std::vector<cpp2::NewEdge>& edges) {
        UNUSED(edges);
        // auto lockKey = ;
    }

    int32_t getPartId(const std::string& src) {
        // auto stPart = mClient_->partId(spaceId_, edgeKey.src.getStr());
        auto stPart = mClient_->partId(spaceId_, src);
        LOG_IF(FATAL, !stPart.ok()) << "mClient_->partId failed";
        return stPart.value();
    }

    /**
     * @brief gen rawkey and partId from EdgeKey
     * @return std::pair<std::string, int32_t> rawkey vs partId
     */
    std::pair<std::string, int32_t> makeRawKey(const cpp2::EdgeKey& e) {
        auto edgeKey = TossTestUtils::toVidKey(e);
        auto partId = getPartId(edgeKey.src.getStr());

        auto ver = 1;
        auto rawKey = TransactionUtils::edgeKey(vIdLen_, partId, edgeKey, ver);
        return std::make_pair(rawKey, partId);
    }

    std::string encodeProps(const cpp2::NewEdge& e) {
        auto edgeType = e.key.get_edge_type();
        auto pSchema = schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType)).get();
        LOG_IF(FATAL, !pSchema) << "Space " << spaceId_ << ", Edge " << edgeType << " invalid";
        auto propNames = makeColNames(e.props.size());
        return encodeRowVal(pSchema, propNames, e.props);
    }

    std::string insertEdge(const cpp2::NewEdge& e) {
        std::string rawKey;
        int32_t partId = 0;
        std::tie(rawKey, partId) = makeRawKey(e.key);
        auto encodedProps = encodeProps(e);
        putValue(rawKey, encodedProps, partId);
        return rawKey;
    }

    cpp2::EdgeKey reverseEdgeKey(const cpp2::EdgeKey& input) {
        cpp2::EdgeKey ret(input);
        std::swap(ret.src, ret.dst);
        ret.edge_type = 0 - ret.edge_type;
        return ret;
    }

    std::string insertReverseEdge(const cpp2::NewEdge& _e) {
        cpp2::NewEdge e(_e);
        e.key = reverseEdgeKey(_e.key);
        return insertEdge(e);
    }

    /**
     * @brief insert a lock according to the given edge e.
     *        also insert reverse edge
     * @return lockKey
     */
    std::string insertLock(const cpp2::NewEdge& e, bool insertInEdge) {
        if (insertInEdge) {
            insertReverseEdge(e);
        }

        std::string rawKey;
        int32_t lockPartId;
        std::tie(rawKey, lockPartId) = makeRawKey(e.key);

        auto lockKey = NebulaKeyUtils::toLockKey(rawKey);
        auto lockVal = encodeProps(e);

        putValue(lockKey, lockVal, lockPartId);

        return lockKey;
    }

    void putValue(std::string key, std::string val, int32_t partId) {
        LOG(INFO) << "put value, partId=" << partId << ", key=" << folly::hexlify(key);
        kvstore::BatchHolder bat;
        bat.put(std::move(key), std::move(val));
        auto batch = encodeBatchValue(bat.getBatch());

        auto txnId = 0;
        auto sf = interClient_->forwardTransaction(txnId, spaceId_, partId, std::move(batch));
        sf.wait();

        if (sf.value() != cpp2::ErrorCode::SUCCEEDED) {
            LOG(FATAL) << "forward txn return=" << static_cast<int>(sf.value());
        }
    }

    bool keyExist(folly::StringPiece key) {
        auto sf = interClient_->getValue(vIdLen_, spaceId_, key);
        sf.wait();
        if (!sf.hasValue()) {
            LOG(FATAL) << "interClient_->getValue has no value";
            return false;
        }
        return nebula::ok(sf.value());
    }

    // simple copy of Storage::BaseProcessor::encodeRowVal
    std::string encodeRowVal(const meta::NebulaSchemaProvider* schema,
                             const std::vector<std::string>& propNames,
                             const std::vector<Value>& props) {
        RowWriterV2 rowWrite(schema);
        WriteResult wRet;
        if (!propNames.empty()) {
            for (size_t i = 0; i < propNames.size(); i++) {
                wRet = rowWrite.setValue(propNames[i], props[i]);
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(FATAL) << "Add field faild";
                }
            }
        } else {
            for (size_t i = 0; i < props.size(); i++) {
                wRet = rowWrite.setValue(i, props[i]);
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(FATAL) << "Add field faild";
                }
            }
        }
        wRet = rowWrite.finish();
        if (wRet != WriteResult::SUCCEEDED) {
            LOG(FATAL) << "Add field faild";
        }

        return std::move(rowWrite).moveEncodedStr();
    }

public:
    std::shared_ptr<folly::IOThreadPoolExecutor>        executor_;
    std::unique_ptr<meta::MetaClient>                   mClient_;
    std::unique_ptr<StorageClient>                      sClient_;
    std::unique_ptr<storage::InternalStorageClient>     interClient_;
    std::unique_ptr<meta::SchemaManager>                schemaMan_;

    int32_t                                             spaceId_{0};
    int32_t                                             edgeType_{0};
    int32_t                                             vIdLen_{0};
};

}  // namespace storage
}  // namespace nebula
