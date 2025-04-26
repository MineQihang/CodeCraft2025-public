#pragma once
#include "disk.hpp"

class DiskRound1 : public Disk {
   public:
    std::vector<int> label_permutation;
    std::vector<std::vector<LabelRegion>> regions;  // 区域
    
    void init(int id_, int num_unit_, int label_num_) override;
    double try_add_object(Object& object, std::vector<int>& units_, int head_id) override;
    int try_add_object_full(Object& object, std::vector<int>& units_, int head_id) override;
    void add_object(Object& object, std::vector<int>& units_, int region_id, int head_id, int start_point) override;
    void delete_units(std::vector<int>& units_) override;
    int get_next_read_step(int head_id, Object* objects, int tolerate_num) override;
    int get_best_jump_point(int head_id, Object* objects) override;
    void do_gc(std::vector<std::pair<int, int>>& swap_ops, Object* objects, int max_swap_num, std::vector<int>& label_permutation) override;
    void do_gc_round1(std::vector<std::pair<int, int>>& swap_ops, Object* objects, int max_swap_num, std::vector<int>& label_permutation, std::map<int, std::set<int>>& need_gc_obj);



    double try_add_object_zero(Object& object, std::vector<int>& units_, int head_id,std::vector<int> lable_list ={});

};

void DiskRound1::init(int id_, int num_unit_, int label_num_) {
    // 初始化数据
    id = id_;
    num_disk_unit = num_unit_;
    label_num = label_num_;
    units.resize(num_disk_unit + 1, 0);
    units.assign(num_disk_unit + 1, 0);
    for (int i = 1; i <= HEAD_NUM; i++) {
        heads[i].point = 1;
        heads[i].last_op = 'j';
        heads[i].last_token = FIRST_READ_TOKEN;
    }
    label_permutation.resize(label_num);
    std::iota(label_permutation.begin(), label_permutation.end(), 1);
    std::shuffle(label_permutation.begin(), label_permutation.end(), std::default_random_engine(23));


    label_permutation = {3, 13, 14, 12, 15, 9, 11, 10, 1, 6, 5, 8, 4, 7, 2, 16 };
    double ratio = NOT_ZERO_RATIO;
    int zero_lable_length = 100;

    std::uniform_real_distribution<> dis(0.6, 1.4);
    gen = std::mt19937(SEED);
    // 根据副本数和标签进行划分
    regions.resize(REP_NUM + 1, std::vector<LabelRegion>(HEAD_NUM + 1));
    int zero_used_length = 0;
    for (int i = 1; i <= REP_NUM; i++) {
        for (int j = 1; j <= HEAD_NUM; j++) {
            auto& region = regions[i][j];
            region.start_point = ((i - 1) * HEAD_NUM + j - 1) * (num_disk_unit / REP_NUM / HEAD_NUM) + 1;
            region.end_point = ((i - 1) * HEAD_NUM + j) * (num_disk_unit / REP_NUM / HEAD_NUM);
            region.length = region.end_point - region.start_point + 1;
            // region.label_points.resize(label_num + 1);
            for (int index = 1 + label_num/2 *(j-1); index <= label_num/2 *j; index++) {
                int label = label_permutation[index - 1];
                auto& label_region = region.label_points[label];
                label_region.length = region.length / label_num * 2* ratio;
                label_region.start_point =
                    (index == 1 + label_num/2 *(j-1)) ? region.start_point
                               : region.zero_regions[label_permutation[index-2]].end_point + 1;
                label_region.end_point = label_region.start_point + label_region.length - 1;
                label_region.write_point = label_region.start_point + label_region.length / 2;
                // std::cerr << "label: " << label << " start: " << label_region.start_point << " end: " << label_region.end_point << std::endl;

                auto& zero_region = region.zero_regions[label];
                zero_region.length = zero_lable_length*dis(gen);
                zero_region.start_point = region.label_points[label].end_point + 1;
                zero_region.end_point = (index == label_num/2 *j) ? region.end_point : zero_region.start_point + zero_region.length - 1;
                // zero_region.write_point = zero_region.start_point + zero_region.length / 2;
                zero_region.write_point = zero_region.start_point;
                if(index == label_num/2 *j){
                    zero_region.length = zero_region.end_point - zero_region.start_point + 1;
                }
                // zero_region.write_point = zero_region.end_point;

                // std::cerr << "zero label: " << label << " start: " << zero_region.start_point << " end: " << zero_region.end_point << std::endl;
            }
        }
        // std::cerr<<std::endl;
    }
}
double DiskRound1::try_add_object_zero(Object& object, std::vector<int>& units_, int head_id, std::vector<int> lable_list){
    auto& region = regions[1][head_id];
    double min_value = INT_MAX;
    std::vector<int> min_value_units;

    std::function<double(int, Region&, std::vector<int>&, bool)> cal_step = [&](int label, Region& zero_region, std::vector<int>& units_inter, bool isOver = false) {
        int current_write_point = zero_region.write_point;
        bool flag = false;

        std::vector<int> write_units;
        int forward_point = current_write_point;
        int forward_step = 0;
        while (write_units.size() < object.size) {
            if (units[forward_point] == 0) {
                // TOFIX: 应该是需要把这个位置变为id的
                write_units.push_back(forward_point);
                if (write_units.size() == object.size) {
                    break;
                }
            }
            if(isOver){
                forward_point = forward_point == region.end_point ? region.start_point
                                                              : forward_point + 1;
            }else{
                forward_point = forward_point == zero_region.end_point ? zero_region.start_point
                                                              : forward_point + 1;
            }
            if ((isOver && forward_point == region.end_point) || (!isOver && forward_point == zero_region.end_point)) {
                flag = true;
            }
            forward_step++;
            if (forward_step >= zero_region.length) {
                // assert(false && "No enough space");
                return 1.0 * INT_MAX;
            }
        }
        // if (flag) forward_step += num_disk_unit;
        units_inter.resize(object.size + 1);
        for (int j = 1; j <= object.size; j++) {
            units_inter[j] = write_units[j - 1];
        }
        return 1.0 * forward_step / zero_region.length;
        // std::vector<int> write_units_back;
        // int back_point = current_write_point;
        // int back_step = 0;
        // flag = false;
        // while (write_units_back.size() < object.size) {
        //     if (units[back_point] == 0) {
        //         write_units_back.push_back(back_point);
        //         if (write_units_back.size() == object.size) {
        //             break;
        //         }
        //     }
        //     if(isOver){
        //         back_point = back_point == region.start_point ? region.end_point
        //                                                       : back_point - 1;
        //     }else{
        //         back_point = back_point == zero_region.start_point ? zero_region.end_point
        //                                                       : back_point - 1;
        //     }
        //     if ((isOver && back_point == region.start_point) || (!isOver && back_point == zero_region.start_point)) {
        //         flag = true;
        //     }

        //     back_step++;
        //     if (back_step >= zero_region.length) {
        //         // assert(false && "No enough space");
        //         return 1.0*INT_MAX;
        //     }
        // }
        // units_inter.resize(object.size + 1);
        // for (int j = 1; j <= object.size; j++) {
        //     units_inter[j] = write_units_back[j - 1];
        // }
        // return 1.0 * back_step / zero_region.length; 

        // if (forward_step < back_step) {
        //     units_inter.resize(object.size + 1);
        //     for (int j = 1; j <= object.size; j++) {
        //         units_inter[j] = write_units[j - 1];
        //     }
        //     return 1.0 * forward_step / region.length;
        // } else {
        //     units_inter.resize(object.size + 1);
        //     for (int j = 1; j <= object.size; j++) {
        //         units_inter[j] = write_units_back[j - 1];
        //     }
        //     return 1.0 * back_step / region.length;
        //     ;
        // }
        return -1.0;
    };

    // 先考虑label_list
    for(auto &label: lable_list){
        auto &zero_region = region.zero_regions[label];
        if(zero_region.length < object.size){
            continue;
        }
        std::vector<int> temp_units;
        double temp_value = cal_step(label, zero_region, temp_units, false);
        if(temp_value < min_value){
            min_value = temp_value;
            min_value_units = temp_units;
        }
        if(temp_value!=INT_MAX){
            units_ = min_value_units;
            object.is_forecast = true;
            return temp_value;
        }
    }

    // 最后考虑其他label
    for(int i = 0;i<label_num;++i){
        int label = label_permutation[i];
        if(std::count(lable_list.begin(),lable_list.end(),label) == 0){
            auto &zero_region = region.zero_regions[label];
            if(zero_region.length < object.size){
                continue;
            }
            std::vector<int> temp_units;
            double temp_value = cal_step(label, zero_region, temp_units, true);
            if(temp_value < min_value){
                min_value = temp_value;
                min_value_units = temp_units;
            }
        }
    }
    units_ = min_value_units;
    return min_value;

}



double DiskRound1::try_add_object(Object& object, std::vector<int>& units_, int head_id) {
    // 找到第一个副本的写入位置
    // if (object.label == 0) {
    //     return try_add_object_zero(object, units_, head_id);
    // }
    auto& region = regions[1][head_id];
    auto label = object.label;
    if (region.label_points.count(label) == 0) {
        return INT_MAX;
    }
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

int DiskRound1::try_add_object_full(Object& object, std::vector<int>& units_, int head_id) {
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

void DiskRound1::add_object(Object& object, std::vector<int>& units_, int region_id, int head_id, int start_point) {
    int size = units_.size();
    auto& region = regions[region_id][head_id];
    for (int j = 1; j < size; j++) {
        if (region_id == 1) {
            int unit_id = units_[j] + region.start_point - start_point;
            assert(units[unit_id] == 0 && "unit_id != 0");
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

void DiskRound1::delete_units(std::vector<int>& units_) {
    int size = units_.size();
    for (int j = 1; j < size; j++) {
        units[units_[j]] = 0;
    }
}

int DiskRound1::get_next_read_step(int head_id, Object* objects, int tolerate_num = 0) {
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

int DiskRound1::get_best_jump_point(int head_id, Object* objects) {
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

void DiskRound1::do_gc(std::vector<std::pair<int, int>>& swap_ops, Object* objects, int max_swap_num, std::vector<int>& label_permutation) {
    // return ;
    // std::cerr<<"id: " << id <<std::endl;
    for (auto& label : label_permutation) {

        for (int head_id = 1; head_id <= HEAD_NUM; head_id++) {
            if(regions[1][head_id].label_points.count(label) == 0) {
                continue;
            }
            // 准备数据
            auto& region1 = regions[1][1];
            auto& region2 = regions[1][2];
            auto& region = regions[1][head_id];

            int search_start = region.start_point;
            int search_end = region.end_point;
            double ratio = 0;
            if(region1.label_points.count(label) != 0) {
                search_start = 1;
                search_end = region2.start_point + region2.length * ratio - 1;
            }
            else if(region2.label_points.count(label) != 0) {
                search_start = region1.start_point + region1.length *(1 - ratio) + 1;
                search_end = region2.end_point;
            }

            int label_start = 0x3f3f3f3f;
            int label_end = 0;
            int label_num = 0;
            for (int i = search_start; i <= search_end; i++) {
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
            // if(region1.label_points.count(label) != 0){
            //     region1.label_points[label].write_point = label_mid;

            // }
            // if(region2.label_points.count(label) != 0){
            //     region2.label_points[label].write_point = label_mid;

            // }
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

            auto swap_op_forecast = [&](int i, int j, bool reverse = false) {
                while ((reverse ? i > j : i < j) && swap_ops.size() < max_swap_num) {
                    while ((reverse ? i > j : i < j) && !(units[i] != 0 && objects[units[i]].label == label && !objects[units[j]].is_known)) {
                        reverse ? i-- : i++;
                    }
                    if(reverse ? i <= j : i >= j) break;
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

            // swap_op_forecast(label_start, label_mid);
            // swap_op_forecast(label_end, label_mid, true);
            
            swap_op(label_start, label_mid);
            swap_op(label_end, label_mid, true);

        }
        
        // std::cerr <<"label: " << label << "swap ops size: " << swap_ops.size() << std::endl;
    }

    // std::cerr << std::endl;
    // std::cerr << "swap ops size: " << swap_ops.size() << std::endl;
}

void DiskRound1::do_gc_round1(std::vector<std::pair<int, int>>& swap_ops, Object* objects, int max_swap_num, std::vector<int>& label_permutation, std::map<int, std::set<int>>& need_gc_obj){
    std::map<int, std::vector<int>> exchanged_obj;
    
    for (auto& label : label_permutation) {
        std::vector<int> handle_id;
        for(auto& obj_id: need_gc_obj[label]){
            if(objects[obj_id].replica[1] == id){
                handle_id.push_back(obj_id);
            }
        }
        std::sort(handle_id.begin(), handle_id.end(), [&](int a, int b){
            return objects[a].size > objects[b].size;
        });

        for (int head_id = 1; head_id <= HEAD_NUM; head_id++) {
            if(regions[1][head_id].label_points.count(label) == 0) {
                continue;
            }
            // 准备数据
            auto& region = regions[1][head_id];
            int new_start = 0x3f3f3f3f;
            int new_end = 0;
            for(int i = region.start_point;i<=region.end_point;++i){
                if (units[i] != 0 && objects[units[i]].label == label&& objects[units[i]].is_known
                || (units[i] != 0 && objects[units[i]].label == label&& !objects[units[i]].is_known && objects[units[i]].is_gc)) {
                    new_start = std::min(new_start, i);
                    new_end = std::max(new_end, i);
                }
            }
            std::vector<int> change_uints;
            for(int i = new_start; i <= new_end; i++){
                if(units[i] == 0){
                    change_uints.push_back(i);
                }         
            }
            int obj_index = 0;
            int change_uint_index = 0;
            while(swap_ops.size() < max_swap_num && obj_index < handle_id.size() && change_uint_index < change_uints.size()){
                Object &obj = objects[handle_id[obj_index]];

                if(obj.size + change_uint_index > change_uints.size()){
                    obj_index++;
                    continue;
                }
                if(swap_ops.size()+obj.size > max_swap_num){
                    obj_index++;
                    continue;
                }

                int replica_id = obj.get_replica_id(id);
                assert(replica_id == 1);
                auto& obj_units = obj.units[replica_id];
                for(int j = 1; j <= obj.size; j++){
                    int x = obj_units[j];
                    int y = change_uints[change_uint_index];

                    obj_units[j] = y;
                    if(units[y] != 0){
                        auto& object2 = objects[units[y]];
                        int replica_id2 = object2.get_replica_id(id);
                        int unit_id2 = object2.get_unit_id(replica_id2, y);
                        object2.units[replica_id2][unit_id2] = x;
                    }
                    units[x] = units[y];
                    units[y] = obj.id;
                    
                    swap_ops.push_back({x, y});
                    change_uint_index++;
                }
                exchanged_obj[label].push_back(obj.id);
                // objects[obj.id].is_gc = true;
                obj_index++;
            }

            region.label_points[label].write_point = new_start + (new_end -new_start)/2;

            
            auto& region1 = regions[1][1];
            auto& region2 = regions[1][2];
            int search_start = region.start_point;
            int search_end = region.end_point;
            double ratio = 0;
            if(region1.label_points.count(label) != 0) {
                search_start = 1;
                search_end = region2.start_point + region2.length * ratio - 1;
            }
            else if(region2.label_points.count(label) != 0) {
                search_start = region1.start_point + region1.length *(1 - ratio) + 1;
                search_end = region2.end_point;
            }

            int label_start = 0x3f3f3f3f;
            int label_end = 0;
            int label_num = 0;
            for (int i = search_start; i <= search_end; i++) {
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
            // if(region1.label_points.count(label) != 0){
            //     region1.label_points[label].write_point = label_mid;

            // }
            // if(region2.label_points.count(label) != 0){
            //     region2.label_points[label].write_point = label_mid;

            // }

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
            
            swap_op(label_end, label_mid, true);
            swap_op(label_start, label_mid);


        }


    }
    for(auto &[label, obj_ids]: exchanged_obj){
        for(auto &obj_id: obj_ids){
            need_gc_obj[label].erase(obj_id);
        }
    }
}






    // return ;
    // std::map<int, std::vector<int>> exchanged_obj;
    // for (auto& label : label_permutation) {
    //     if(need_gc_obj.count(label) == 0){
    //         continue;
    //     }
    //     std::vector<int> handle_id;
    //     for(auto& obj_id: need_gc_obj[label]){
    //         if(objects[obj_id].replica[1] == id){
    //             handle_id.push_back(obj_id);
    //         }
    //     }
    //     std::sort(handle_id.begin(), handle_id.end(), [&](int a, int b){
    //         return objects[a].request_queue.size() > objects[b].request_queue.size();
    //     });
    //     for (int head_id = 1; head_id <= HEAD_NUM; head_id++) {
    //         if(regions[1][head_id].label_points.count(label) == 0){
    //             continue;
    //         }
    //         auto& region = regions[1][head_id].label_points[label];
    //         int label_start = region.start_point;
    //         int label_end = region.end_point;
    //         std::vector<int> change_uints;
    //         for(int i = label_start; i <= label_end; i++){
    //             if(units[i] == 0 || (units[i] != 0 && objects[units[i]].label != label)){
    //             // if(units[i] == 0){
    //                 change_uints.push_back(i);
    //             }         
    //         }
    //         // for(int i = regions[1][head_id].zero_regions[label].start_point; i <= regions[1][head_id].zero_regions[label].end_point; i++){
    //         //     if(units[i] == 0 || (units[i] != 0 && objects[units[i]].label != label)){
    //         //     // if(units[i] == 0){
    //         //     if(std::find(change_uints.begin(), change_uints.end(), i) != change_uints.end()){
    //         //         std::cerr << "no" << std::endl;}
    //         //         change_uints.push_back(i);
    //         //     }
    //         // }
    //         int obj_index = 0;
    //         int change_uint_index = 0;
    //         while(swap_ops.size() < max_swap_num && obj_index < handle_id.size() && change_uint_index < change_uints.size()){
    //             Object &obj = objects[handle_id[obj_index]];

    //             if(obj.size + change_uint_index > change_uints.size()){
    //                 obj_index++;
    //                 continue;
    //             }
    //             if(swap_ops.size()+obj.size > max_swap_num){
    //                 obj_index++;
    //                 continue;
    //             }

    //             int replica_id = obj.get_replica_id(id);
    //             assert(replica_id == 1);
    //             auto& obj_units = obj.units[replica_id];
    //             for(int j = 1; j <= obj.size; j++){
    //                 int x = obj_units[j];
    //                 assert(change_uint_index < change_uints.size());
    //                 int y = change_uints[change_uint_index];
    //                 //assert(x >= regions[1][head_id].start_point && x <= regions[1][head_id].end_point);
    //                 assert(y >= regions[1][head_id].start_point && y <= regions[1][head_id].end_point);
    //                 if (units[x] != obj.id) {
    //                     std::cerr << units[x] << " " << obj.id << " " << objects[units[x]].id << std::endl;
    //                 }
    //                 assert(units[x] == obj.id);

    //                 // int unit_id = obj.get_unit_id(replica_id, x);
    //                 // assert(unit_id == j);
    //                 obj_units[j] = y;
    //                 if(units[y] != 0){
    //                     auto& object2 = objects[units[y]];
    //                     assert(object2.id == units[y]);
    //                     int replica_id2 = object2.get_replica_id(id);
    //                     assert(replica_id2 == 1);
    //                     int unit_id2 = object2.get_unit_id(replica_id2, y);
    //                     object2.units[replica_id2][unit_id2] = x;
    //                     assert(obj.id != object2.label);
    //                 }
    //                 units[x] = units[y];
    //                 units[y] = obj.id;
                    
    //                 swap_ops.push_back({x, y});
    //                 change_uint_index++;
    //             }
    //             exchanged_obj[label].push_back(obj.id);
    //             obj_index++;
    //         }
    //     }
    //     if(swap_ops.size() >= max_swap_num){
    //         break;
    //     }
    // }
    // for(auto &[label, obj_ids]: exchanged_obj){
    //     for(auto &obj_id: obj_ids){
    //         need_gc_obj[label].erase(obj_id);
    //     }
    // }
