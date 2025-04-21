#pragma once
#include "object.hpp"
#include "utils.hpp"

class Region {
   public:
    int start_point;  // 区域起始位置
    int end_point;    // 区域结束位置
    int write_point;  // 区域写入位置
    int length;       // 区域长度
};

class LabelRegion : public Region {
   public:
    std::vector<Region> label_points;  // 标签对应的位置
};

class Head {
   public:
    int point;       // 当前读取位置
    char last_op;    // 上一次操作
    int last_token;  // 上一次操作的token
};

class Disk {
   public:
    int id;                                         // 磁盘编号
    int num_disk_unit;                              // 磁盘单元数量
    std::vector<int> units;                         // 磁盘单元
    Head heads[HEAD_NUM + 1];                       // 读写头
    std::vector<std::vector<LabelRegion>> regions;  // 区域
    int label_num;                                  // 标签数量

    void init(int id_, int num_unit_, std::vector<double>& label_num_norm,
              std::vector<int>& label_permutation);
    double try_add_object(Object& object, std::vector<int>& units_, int head_id);
    int try_add_object_full(Object& object, std::vector<int>& units_, int head_id);
    void add_object(Object& object, std::vector<int>& units_, int region_id, int head_id, int start_point);
    void delete_units(std::vector<int>& units_);
    int get_next_read_step(int head_id, Object* objects, int tolerate_num);
    int get_best_jump_point(int head_id, Object* objects);
    void do_gc(std::vector<std::pair<int, int>>& swap_ops, Object* objects, int max_swap_num, std::vector<int>& label_permutation);
};

void Disk::init(int id_, int num_unit_, std::vector<double>& label_num_norm,
                std::vector<int>& label_permutation) {
    // 初始化数据
    id = id_;
    num_disk_unit = num_unit_;
    units.resize(num_disk_unit + 1, 0);
    for (int i = 1; i <= HEAD_NUM; i++) {
        heads[i].point = 1;
        heads[i].last_op = 'j';
        heads[i].last_token = FIRST_READ_TOKEN;
    }

    // 根据副本数和标签进行划分
    label_num = label_num_norm.size() - 1;
    regions.resize(REP_NUM + 1, std::vector<LabelRegion>(HEAD_NUM + 1));
    for (int i = 1; i <= REP_NUM; i++) {
        for (int j = 1; j <= HEAD_NUM; j++) {
            auto& region = regions[i][j];
            region.start_point = ((i - 1) * HEAD_NUM + j - 1) * (num_disk_unit / REP_NUM / HEAD_NUM) + 1;
            region.end_point = ((i - 1) * HEAD_NUM + j) * (num_disk_unit / REP_NUM / HEAD_NUM);
            region.length = region.end_point - region.start_point + 1;
            region.label_points.resize(label_num + 1);
            for (int k = 0; k < label_num; k++) {
                auto label = label_permutation[k];
                auto& label_region = region.label_points[label];
                label_region.length = label_num_norm[label] * region.length;
                label_region.start_point =
                    k == 0 ? region.start_point
                           : region.label_points[label_permutation[k - 1]].end_point + 1;
                label_region.end_point = label_region.start_point + label_region.length - 1;
                label_region.write_point = label_region.start_point + label_region.length / 2;
            }
        }
    }
}

double Disk::try_add_object(Object& object, std::vector<int>& units_, int head_id) {
    // 找到第一个副本的写入位置
    auto& region = regions[1][head_id];
    auto label = object.label;
    if (region.label_points[label].length < object.size) {
        return INT_MAX;
    }
    int current_write_point = region.label_points[label].write_point;

    // 开始尝试往前写入
    std::vector<int> write_units;
    int forward_point = current_write_point;
    int forward_step = 0;
    bool flag = false;
    while (write_units.size() < object.size) {
        if (units[forward_point] == 0) {
            // TOFIX: 应该是需要把这个位置变为id的
            write_units.push_back(forward_point);
            if (write_units.size() == object.size) {
                break;
            }
        }
        forward_point = forward_point == region.end_point ? region.start_point
                                                          : forward_point + 1;
        if (forward_point == region.label_points[label].end_point) {
            flag = true;
        }
        forward_step++;
        if (forward_step >= region.length) {
            // assert(false && "No enough space");
            return INT_MAX;
        }
    }
    // if (flag) forward_step += num_disk_unit;

    // 开始尝试往后写入
    std::vector<int> write_units_back;
    int back_point = current_write_point;
    int back_step = 0;
    flag = false;
    while (write_units_back.size() < object.size) {
        if (units[back_point] == 0) {
            write_units_back.push_back(back_point);
            if (write_units_back.size() == object.size) {
                break;
            }
        }
        back_point = back_point == region.start_point ? region.end_point
                                                      : back_point - 1;
        if (back_point == region.label_points[label].start_point) {
            flag = true;
        }
        back_step++;
        if (back_step >= region.length) {
            // assert(false && "No enough space");
            return INT_MAX;
        }
    }
    // if (flag) back_step += num_disk_unit;

    // 选择写入方式
    if (forward_step < back_step) {
        units_.resize(object.size + 1);
        for (int j = 1; j <= object.size; j++) {
            units_[j] = write_units[j - 1];
        }
        return 1.0 * forward_step / region.length;
    } else {
        units_.resize(object.size + 1);
        for (int j = 1; j <= object.size; j++) {
            units_[j] = write_units_back[j - 1];
        }
        return 1.0 * back_step / region.length;
        ;
    }
    return -1;
}

// 一个obj只能整块放到一个区域
int Disk::try_add_object_full(Object& object, std::vector<int>& units_, int head_id) {
    // 找到第一个副本的写入位置
    auto& region = regions[1][head_id];
    auto label = object.label;
    int current_write_point = region.label_points[label].write_point;

    // 开始尝试往前写入
    std::vector<int> write_units;
    int forward_point = current_write_point;
    int forward_step = 0;
    // 看看现在read指针在不在这个区域内
    bool flag = false;
    // 滑动窗口找到第一个能放下的位置
    int window_size = object.size;
    int window_not_free_num = 0;
    int temp_point = forward_point;
    for (int i = 0; i < window_size; i++) {
        if (units[forward_point] != 0) {
            window_not_free_num++;
        }
        forward_point = forward_point == region.end_point ? region.start_point
                                                          : forward_point + 1;
        forward_step++;
        if (forward_step >= region.length) {
            assert(false && "No enough space");
        }
    }
    while (window_not_free_num > 0) {
        if (units[temp_point] != 0) {
            window_not_free_num--;
        }
        if (units[forward_point] != 0) {
            window_not_free_num++;
        }
        forward_point = forward_point == region.end_point ? region.start_point
                                                          : forward_point + 1;
        temp_point = temp_point == region.end_point ? region.start_point
                                                    : temp_point + 1;
        forward_step++;
        if (forward_step >= region.length) {
            assert(false && "No enough space");
        }
    }
    for (int i = 0; i < window_size; i++) {
        write_units.push_back(temp_point);
        temp_point = temp_point == region.end_point ? region.start_point
                                                    : temp_point + 1;
    }

    // 开始尝试往后写入
    std::vector<int> write_units_back;
    int back_point = current_write_point;
    int back_step = 0;
    // 滑动窗口找到第一个能放下的位置
    window_size = object.size;
    window_not_free_num = 0;
    temp_point = back_point;
    for (int i = 0; i < window_size; i++) {
        if (units[back_point] != 0) {
            window_not_free_num++;
        }
        back_point = back_point == region.start_point ? region.end_point
                                                      : back_point - 1;
        back_step++;
        if (back_step >= region.length) {
            assert(false && "No enough space");
        }
    }
    while (window_not_free_num > 0) {
        if (units[temp_point] != 0) {
            window_not_free_num--;
        }
        if (units[back_point] != 0) {
            window_not_free_num++;
        }
        back_point = back_point == region.start_point ? region.end_point
                                                      : back_point - 1;
        temp_point = temp_point == region.start_point ? region.end_point
                                                      : temp_point - 1;
        back_step++;
        if (back_step >= region.length) {
            assert(false && "No enough space");
        }
    }
    for (int i = 0; i < window_size; i++) {
        write_units_back.push_back(temp_point);
        temp_point = temp_point == region.start_point ? region.end_point
                                                      : temp_point - 1;
    }

    // 选择写入方式
    if (forward_step < back_step) {
        units_.resize(object.size + 1);
        for (int j = 1; j <= object.size; j++) {
            units_[j] = write_units[j - 1];
        }
        return forward_step;
    } else {
        units_.resize(object.size + 1);
        for (int j = 1; j <= object.size; j++) {
            units_[j] = write_units_back[j - 1];
        }
        return back_step;
    }
    return -1;
}

void Disk::add_object(Object& object, std::vector<int>& units_, int region_id, int head_id, int start_point) {
    int size = units_.size();
    auto& region = regions[region_id][head_id];
    for (int j = 1; j < size; j++) {
        if (region_id == 1) {
            int unit_id = units_[j] + region.start_point - start_point;
            units[unit_id] = object.id;
            object.units[region_id][j] = unit_id;
            continue;
        }
        int unit_id = region.end_point - (units_[j] - start_point);  // region.start_point + (units_[j] - start_point);
        while (units[unit_id] != 0) {
            unit_id = unit_id == region.end_point ? region.start_point : unit_id + 1;
        }
        units[unit_id] = object.id;
        object.units[region_id][j] = unit_id;
    }
    object.replica[region_id] = id;
}

void Disk::delete_units(std::vector<int>& units_) {
    int size = units_.size();
    for (int j = 1; j < size; j++) {
        units[units_[j]] = 0;
    }
}

int Disk::get_next_read_step(int head_id, Object* objects, int tolerate_num = 0) {
    int start_point = heads[head_id].point;
    int end_point = regions[1][head_id].end_point;
    for (int step = 0;
         step < num_disk_unit && start_point <= end_point;
         step++) {
        if (units[start_point] != 0 &&
            objects[units[start_point]].request_queue.size() > 0) {
            tolerate_num--;
            if (tolerate_num <= 0) {
                return step;
            }
        }
        start_point = FORWARD_STEP(start_point);
    }
    return -1;
};

int Disk::get_best_jump_point(int head_id, Object* objects) {
    int start_point = regions[1][head_id].start_point;
    int end_point = regions[1][head_id].end_point;
    for (int step = 0;
         step < num_disk_unit && start_point <= end_point;
         step++) {
        if (units[start_point] != 0 &&
            objects[units[start_point]].request_queue.size() > 0) {
            return start_point;
        }
        start_point = FORWARD_STEP(start_point);
    }
    return regions[1][head_id].start_point;
}

void Disk::do_gc(std::vector<std::pair<int, int>>& swap_ops, Object* objects, int max_swap_num, std::vector<int>& label_permutation) {
    // return ;
    for (auto& label : label_permutation) {
        for (int head_id = 1; head_id <= HEAD_NUM; head_id++) {
            // 准备数据
            auto& region = regions[1][head_id];
            int label_start = 0x3f3f3f3f;
            int label_end = 0;
            int label_num = 0;
            for (int i = region.start_point; i <= region.end_point; i++) {
                if (units[i] != 0 && objects[units[i]].label == label) {
                    label_start = std::min(label_start, i);
                    label_end = std::max(label_end, i);
                    label_num++;
                }
            }
            int label_mid = 0;
            int temp_num = 0;
            for (int i = label_start; i <= label_end; i++) {
                if (units[i] != 0 && objects[units[i]].label == label) {
                    temp_num++;
                    if (temp_num == label_num / 2) {
                        label_mid = i;
                        break;
                    }
                }
            }
            if (label_mid == 0) {
                continue;
            }
            region.label_points[label].write_point = label_mid;
            // 区域内交换
            auto swap_op = [&](int i, int j, bool reverse = false) {
                while ((reverse ? i > j : i < j) && swap_ops.size() < max_swap_num) {
                    while (!(units[i] != 0 && objects[units[i]].label == label)) {
                        reverse ? i-- : i++;
                    }
                    while ((units[j] != 0 && objects[units[j]].label == label)) {
                        reverse ? j++ : j--;
                    }
                    if (reverse ? i <= j : i >= j) break;
                    // i现在是label的，j现在是空的/其他label的
                    auto& object = objects[units[i]];
                    int replica_id = object.get_replica_id(id);
                    int unit_id = object.get_unit_id(replica_id, i);
                    object.units[replica_id][unit_id] = j;

                    // 如果j不是空的，说明是其他label的
                    if (units[j] != 0) {
                        auto& object2 = objects[units[j]];
                        int replica_id2 = object2.get_replica_id(id);
                        int unit_id2 = object2.get_unit_id(replica_id2, j);
                        object2.units[replica_id2][unit_id2] = i;
                    }

                    std::swap(units[i], units[j]);
                    swap_ops.push_back({i, j});
                    reverse ? i-- : i++;
                    reverse ? j++ : j--;
                }
            };
            swap_op(label_start, label_mid);
            swap_op(label_end, label_mid, true);
        }
    }
}
