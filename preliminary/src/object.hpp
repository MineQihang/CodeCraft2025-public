#pragma once
#include "request.hpp"
#include "utils.hpp"

class Object {
   public:
    int id;                                     // 对象id
    int size;                                   // 对象大小
    int label;                                  // 对象标签
    bool is_delete;                             // 是否被删除
    int replica[REP_NUM + 1];                   // 对象的副本
    std::vector<int> units[REP_NUM + 1];        // 对象的单元
    std::vector<int> unit_last_read_timestamp;  // 对象的单元最后一次读取时间戳
    int last_read_timestamp;                    // 对象的最后一次完整读取时间戳
    bool lazy;                                  // 懒更新最小时间戳
    std::queue<int> request_queue;              // 对象的请求队列

    // 操作函数
    void init(int id, int size, int label);
    void delete_();
    int get_replica_id(int disk_id);
    int get_unit_id(int replica_id, int unit_id);
    void read(int disk_id, int disk_unit_id, int timestamp);
    int get_last_all_read_timestamp();
    double get_approximate_score();
    double get_score(int timestamp, Request* requests);
};

void Object::init(int id, int size, int label) {
    this->id = id;
    this->size = size;
    this->label = label;
    this->is_delete = false;
    unit_last_read_timestamp.resize(size + 1, 0);
    last_read_timestamp = 0;
    lazy = true;
    for (int i = 1; i <= REP_NUM; i++) {
        units[i].resize(size + 1, 0);
    }
}

void Object::delete_() { is_delete = true; }

int Object::get_replica_id(int disk_id) {
    for (int i = 1; i <= REP_NUM; i++) {
        if (replica[i] == disk_id) {
            return i;
        }
    }
    return -1;
}

int Object::get_unit_id(int replica_id, int unit_id) {
    for (int i = 1; i <= units[replica_id].size(); i++) {
        if (units[replica_id][i] == unit_id) {
            return i;
        }
    }
    return -1;
}

void Object::read(int disk_id, int disk_unit_id, int timestamp) {
    auto replica_id = get_replica_id(disk_id);
    auto unit_id = get_unit_id(replica_id, disk_unit_id);
    assert(unit_id != -1 && "unit_id == -1");
    lazy = (lazy || unit_last_read_timestamp[unit_id] == last_read_timestamp);
    unit_last_read_timestamp[unit_id] = timestamp;
}

int Object::get_last_all_read_timestamp() {
    if (lazy) {
        last_read_timestamp = INT_MAX;
        for (int i = 1; i <= size; i++) {
            last_read_timestamp =
                std::min(last_read_timestamp, unit_last_read_timestamp[i]);
        }
        lazy = false;
    }
    return last_read_timestamp;
}

double Object::get_approximate_score() { return request_queue.size(); }

double Object::get_score(int timestamp, Request* requests) {
    auto temp_queue = request_queue;
    double score = 0;
    while (!temp_queue.empty()) {
        int t = timestamp - requests[temp_queue.front()].start_timestamp;
        score +=
            t <= 10 ? (-0.005 * t + 1) : (t <= 105 ? (-0.01 * t + 1.05) : 0);
        temp_queue.pop();
    }
    score *= (size + 1) * 0.5;
    return score;
}