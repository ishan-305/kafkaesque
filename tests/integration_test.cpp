#include <gtest/gtest.h>

#include <streamlog/broker.h>
#include <streamlog/consumer.h>
#include <streamlog/producer.h>

#include <filesystem>

using namespace streamlog;

namespace {
std::string temp_dir(const std::string& name) {
    auto p = std::filesystem::temp_directory_path() / ("streamlog_it_" + name);
    std::filesystem::remove_all(p);
    return p.string();
}
}

TEST(Integration, ProduceThenFetchOverTcp) {
    Broker broker({.data_dir = temp_dir("basic")});
    ASSERT_EQ(broker.start(0), Status::OK);

    Producer producer;
    ASSERT_EQ(producer.connect("127.0.0.1", broker.port()), Status::OK);
    for (int i = 0; i < 100; ++i) {
        uint64_t offset = 999;
        ASSERT_EQ(producer.send("orders", "key-" + std::to_string(i),
                                "value-" + std::to_string(i), offset),
                  Status::OK);
        EXPECT_EQ(offset, static_cast<uint64_t>(i));
    }

    Consumer consumer;
    ASSERT_EQ(consumer.connect("127.0.0.1", broker.port()), Status::OK);
    ASSERT_EQ(consumer.subscribe("orders", 0), Status::OK);
    EXPECT_EQ(consumer.position(), 0u);  // never committed → offset 0

    std::vector<Record> records;
    while (consumer.position() < consumer.high_watermark()) {
        ASSERT_EQ(consumer.poll(records), Status::OK);
    }
    ASSERT_EQ(records.size(), 100u);
    for (size_t i = 0; i < records.size(); ++i) {
        EXPECT_EQ(records[i].key, "key-" + std::to_string(i));
        EXPECT_EQ(records[i].value, "value-" + std::to_string(i));
    }

    broker.stop();
}

// THE PHASE 1 MILESTONE: kill the broker mid-run, restart it on the same data
// dir, and fetch everything back from offset 0.
TEST(Integration, BrokerRestartFetchFromZero) {
    std::string dir = temp_dir("restart");

    {
        Broker broker({.data_dir = dir});
        ASSERT_EQ(broker.start(0), Status::OK);
        Producer producer;
        ASSERT_EQ(producer.connect("127.0.0.1", broker.port()), Status::OK);
        for (int i = 0; i < 500; ++i) {
            uint64_t offset;
            ASSERT_EQ(producer.send("events", "k" + std::to_string(i),
                                    "v" + std::to_string(i), offset),
                      Status::OK);
        }
        broker.stop();  // "kill" the broker
    }

    Broker broker({.data_dir = dir});  // restart on the same data dir
    ASSERT_EQ(broker.start(0), Status::OK);

    // Producing resumes at the recovered offset counter.
    Producer producer;
    ASSERT_EQ(producer.connect("127.0.0.1", broker.port()), Status::OK);
    uint64_t offset = 0;
    ASSERT_EQ(producer.send("events", "k500", "v500", offset), Status::OK);
    EXPECT_EQ(offset, 500u);

    // Consuming from offset 0 returns every message in order.
    Consumer consumer;
    ASSERT_EQ(consumer.connect("127.0.0.1", broker.port()), Status::OK);
    ASSERT_EQ(consumer.subscribe("events", 0), Status::OK);
    consumer.seek(0);
    std::vector<Record> records;
    while (consumer.position() < consumer.high_watermark()) {
        ASSERT_EQ(consumer.poll(records), Status::OK);
    }
    ASSERT_EQ(records.size(), 501u);
    for (size_t i = 0; i < records.size(); ++i) {
        EXPECT_EQ(records[i].value, "v" + std::to_string(i)) << "offset " << i;
    }

    broker.stop();
}

// Committed offsets survive a broker restart: the consumer resumes where the
// group left off instead of re-reading.
TEST(Integration, CommittedOffsetSurvivesRestart) {
    std::string dir = temp_dir("commit");

    {
        Broker broker({.data_dir = dir});
        ASSERT_EQ(broker.start(0), Status::OK);
        Producer producer;
        ASSERT_EQ(producer.connect("127.0.0.1", broker.port()), Status::OK);
        for (int i = 0; i < 20; ++i) {
            uint64_t offset;
            ASSERT_EQ(producer.send("jobs", "", "job-" + std::to_string(i), offset),
                      Status::OK);
        }

        Consumer consumer;
        ASSERT_EQ(consumer.connect("127.0.0.1", broker.port()), Status::OK);
        ASSERT_EQ(consumer.subscribe("jobs", 0, "workers"), Status::OK);
        std::vector<Record> records;
        // Consume exactly the first batch and commit.
        ASSERT_EQ(consumer.poll(records, 200), Status::OK);
        ASSERT_GT(records.size(), 0u);
        ASSERT_LT(records.size(), 20u);  // small max_bytes → partial read
        ASSERT_EQ(consumer.commit(), Status::OK);
        broker.stop();
    }

    Broker broker({.data_dir = dir});
    ASSERT_EQ(broker.start(0), Status::OK);

    Consumer consumer;
    ASSERT_EQ(consumer.connect("127.0.0.1", broker.port()), Status::OK);
    ASSERT_EQ(consumer.subscribe("jobs", 0, "workers"), Status::OK);
    EXPECT_GT(consumer.position(), 0u);  // resumed from the durable commit

    std::vector<Record> rest;
    uint64_t resume_at = consumer.position();
    while (consumer.position() < consumer.high_watermark()) {
        ASSERT_EQ(consumer.poll(rest), Status::OK);
    }
    ASSERT_EQ(rest.size(), 20u - resume_at);
    EXPECT_EQ(rest.front().value, "job-" + std::to_string(resume_at));
    EXPECT_EQ(rest.back().value, "job-19");

    broker.stop();
}

// Multi-partition topic: explicit-partition produce/fetch round-trip and
// key routing stability.
TEST(Integration, MultiPartitionTopic) {
    Broker broker({.data_dir = temp_dir("multi")});
    ASSERT_EQ(broker.start(0), Status::OK);
    ASSERT_EQ(broker.topics().create_topic("metrics", 4), Status::OK);

    Producer producer;
    ASSERT_EQ(producer.connect("127.0.0.1", broker.port()), Status::OK);

    // Explicit partitions: each gets its own offset sequence.
    for (uint32_t p = 0; p < 4; ++p) {
        for (int i = 0; i < 5; ++i) {
            uint64_t offset;
            ASSERT_EQ(producer.send_to("metrics", static_cast<int32_t>(p),
                                       "k", "p" + std::to_string(p) + "-" +
                                       std::to_string(i), offset),
                      Status::OK);
            EXPECT_EQ(offset, static_cast<uint64_t>(i));
        }
    }

    // Key routing: same key always lands on the same partition —
    // read back all partitions and check each key's records are on one.
    uint64_t off;
    ASSERT_EQ(producer.send("metrics", "stable-key", "first", off), Status::OK);
    ASSERT_EQ(producer.send("metrics", "stable-key", "second", off), Status::OK);

    int partitions_with_key = 0;
    for (uint32_t p = 0; p < 4; ++p) {
        Consumer consumer;
        ASSERT_EQ(consumer.connect("127.0.0.1", broker.port()), Status::OK);
        ASSERT_EQ(consumer.subscribe("metrics", p), Status::OK);
        std::vector<Record> records;
        while (consumer.position() < consumer.high_watermark()) {
            ASSERT_EQ(consumer.poll(records), Status::OK);
        }
        int key_hits = 0;
        for (const Record& r : records) {
            if (r.key == "stable-key") ++key_hits;
        }
        if (key_hits > 0) {
            EXPECT_EQ(key_hits, 2);  // both landed together
            ++partitions_with_key;
        }
    }
    EXPECT_EQ(partitions_with_key, 1);

    // Out-of-range partition rejected.
    EXPECT_EQ(producer.send_to("metrics", 99, "k", "v", off), Status::NOT_FOUND);

    broker.stop();
}
