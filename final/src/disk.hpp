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
    std::map<int, Region> label_points;  // 标签对应的位置
    std::map<int, Region> zero_regions;  // 0标签对应的位置
};

class Head {
   public:
    int point;       // 当前读取位置
    char last_op;    // 上一次操作
    int last_token;  // 上一次操作的token
};

class Disk {
   public:
    int id;                    // 磁盘编号
    int num_disk_unit;         // 磁盘单元数量
    std::vector<int> units;    // 磁盘单元
    Head heads[HEAD_NUM + 1];  // 读写头
    int label_num;             // 标签数量

    std::mt19937 gen;               // 随机数生成器

    virtual void init(int id_, int num_unit_, int label_num_) = 0;
    virtual double try_add_object(Object& object, std::vector<int>& units_, int head_id) = 0;
    virtual int try_add_object_full(Object& object, std::vector<int>& units_, int head_id) = 0;
    virtual void add_object(Object& object, std::vector<int>& units_, int region_id, int head_id, int start_point) = 0;
    virtual void delete_units(std::vector<int>& units_) = 0;
    virtual int get_next_read_step(int head_id, Object* objects, int tolerate_num) = 0;
    virtual int get_best_jump_point(int head_id, Object* objects) = 0;
    virtual void do_gc(std::vector<std::pair<int, int>>& swap_ops, Object* objects, int max_swap_num, std::vector<int>& label_permutation) = 0;
};