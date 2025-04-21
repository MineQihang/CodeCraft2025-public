#pragma once
#include "controller.hpp"

void Controller::do_object_delete_round2(Object& object) {
    for (int i = 1; i <= REP_NUM; i++) {
        auto& disk = disks2[object.replica[i]];
        disk.delete_units(object.units[i]);
    }
    object.delete_(current_timestamp);
}

void Controller::do_object_write_round2(Object& object, int prv_disk = -1) {
    // 找出放到哪个磁盘上
    double min_value = INT_MAX;
    int min_value_disk;
    int min_value_head;
    std::vector<int> min_value_units;
    for (int i = 1; i <= num_disk; i++) {
        for (int j = 1; j <= HEAD_NUM; j++) {
            if (prv_disk != -1 && i != prv_disk) {
                continue;
            }
            auto& disk = disks2[i];
            std::vector<int> units;
            double value = disk.try_add_object(object, units, j);
            if (value < min_value) {
                min_value = value;
                min_value_disk = i;
                min_value_units = units;
                min_value_head = j;
            }
        }
    }

    // 写入磁盘
    int start_point = disks1[min_value_disk].regions[1][min_value_head].start_point;
    for (int i = 1; i <= REP_NUM; i++) {
        auto& disk = disks2[(min_value_disk + i - 2) % num_disk + 1];
        disk.add_object(object, min_value_units, i, min_value_head, start_point);
        if (i == 1) stat_obj_num[min_value_disk][min_value_head] += 1;
    }
}

void Controller::do_request_add_round2(Request& request, Object& object) {
    request.object_id = object.id;
    request.start_timestamp = current_timestamp;
    if (need_calibrate_obj.count(object.id)) {
        request.is_done = true;
        busy_requests.push_back(request.id);
        return;
    }
    request.is_done = false;
    object.request_queue.push(request.id);
    delete_queue.push(request.id);
}

void Controller::do_disk_read_round2(Disk& disk, int head_id, std::stringstream& action_ss) {
    auto& disk_unit = disk.units;
    auto& disk_point = disk.heads[head_id].point;
    auto& disk_last_op = disk.heads[head_id].last_op;
    auto& disk_last_token = disk.heads[head_id].last_token;

    // 判断是不是该跳转了
    int step = disk.get_next_read_step(head_id, objects, 0);
    if (step >= max_num_token && disk_point + step <= disks1[disk.id].regions[1][head_id].end_point) {
        action_ss << "j " << disk_point + step << "\n";
        disk_point = disk_point + step;
        disk_last_op = 'j';
        disk_last_token = FIRST_READ_TOKEN;
        return;
    } else if (step == -1) {
        stat_read_speed[disk.id][head_id].push_back(current_timestamp);
        int best_point = disk.get_best_jump_point(head_id, objects);
        action_ss << "j " << best_point << "\n";
        disk_point = best_point;
        disk_last_op = 'j';
        disk_last_token = FIRST_READ_TOKEN;
        return;
    }

    // 暴力算最好的r-p策略
    std::function<double(int, int, char, int, bool, std::string&)> calc_rp = [&](int current_point, int current_token, char last_op, int last_token, bool last_empty, std::string& action) {
        if (current_token <= 0) {
            // 计算分数
            double score = 0;
            int n = action.size();
            for (int i = n - 1; i >= 0; i--) {
                current_point = BACKWARD_STEP(current_point);
                if (action[i] == 'r' && disk_unit[current_point] != 0 && objects[disk_unit[current_point]].request_queue.size() > 0) {
                    score += 1.0 * objects[disk_unit[current_point]].request_queue.size() / objects[disk_unit[current_point]].size;
                }
            }
            // if (last_op == 'r') {
            //     score += (1 - 1.0 * last_token / FIRST_READ_TOKEN) * 0.5;
            // }
            return 1.0 * action.size();
        }
        if (disk_unit[current_point] == 0 || objects[disk_unit[current_point]].request_queue.size() == 0) {
            if (last_op != 'r') {
                while (current_token > 0) {
                    action += "p";
                    current_token -= 1;
                    current_point = FORWARD_STEP(current_point);
                    if (disk_unit[current_point] != 0 && objects[disk_unit[current_point]].request_queue.size() > 0) {
                        break;
                    }
                }
                return calc_rp(
                    current_point,
                    current_token,
                    'p',
                    FIRST_READ_TOKEN,
                    true,
                    action);
            } else {
                // PASS
                int temp_point = current_point;
                int temp_token = current_token;
                int temp_last_token = last_token;
                char temp_last_op = last_op;
                std::string temp_action = action;
                while (temp_token > 0) {
                    temp_action += "p";
                    temp_token -= 1;
                    temp_point = FORWARD_STEP(temp_point);
                    if (disk_unit[temp_point] != 0 && objects[disk_unit[temp_point]].request_queue.size() > 0) {
                        break;
                    }
                }
                double pass_score = calc_rp(
                    temp_point,
                    temp_token,
                    'p',
                    FIRST_READ_TOKEN,
                    true,
                    temp_action);
                if (temp_token <= 0) {
                    action = temp_action;
                    return pass_score;
                }

                // READ
                std::string read_action = action;
                bool flag = false;
                while (true) {
                    int use_token = last_op == 'r' ? NEXT_TOKEN(last_token) : FIRST_READ_TOKEN;
                    if (current_token < use_token) {
                        flag = true;
                        break;
                    }
                    read_action += "r";
                    current_token -= use_token;
                    current_point = FORWARD_STEP(current_point);
                    last_op = 'r';
                    last_token = use_token;
                    if (disk_unit[current_point] != 0 && objects[disk_unit[current_point]].request_queue.size() > 0) {
                        break;
                    }
                }
                double read_score = calc_rp(
                    current_point,
                    flag ? 0 : current_token,
                    last_op,
                    last_token,
                    true,
                    read_action);
                if (read_score > pass_score) {
                    action = read_action;
                    return read_score;
                } else if (read_score < pass_score) {
                    action = temp_action;
                    return pass_score;
                } else {
                    if (read_action.size() < temp_action.size()) {
                        action = temp_action;
                        return pass_score;
                    } else {
                        action = read_action;
                        return read_score;
                    }
                }
            }
        } else {
            // READ
            bool flag = false;
            while (true) {
                int use_token = last_op == 'r' ? NEXT_TOKEN(last_token) : FIRST_READ_TOKEN;
                if (current_token < use_token) {
                    flag = true;
                    break;
                }
                action += "r";
                current_token -= use_token;
                current_point = FORWARD_STEP(current_point);
                last_op = 'r';
                last_token = use_token;
                if (disk_unit[current_point] == 0 || objects[disk_unit[current_point]].request_queue.size() == 0) {
                    break;
                }
            }
            return calc_rp(
                current_point,
                flag ? 0 : current_token,
                last_op,
                last_token,
                false,
                action);
        }
    };
    std::string best_action;
    bool last_empty_ = disk_unit[BACKWARD_STEP(disk_point)] == 0 || objects[disk_unit[BACKWARD_STEP(disk_point)]].request_queue.size() == 0;
    calc_rp(disk_point, max_num_token * TOKEN_CONSIDERED_RATE, disk_last_op, disk_last_token, last_empty_, best_action);

    // 根据action_ss更新状态
    int valid_token = max_num_token;
    for (int i = 0; i < best_action.size(); i++) {
        if (best_action[i] == 'r') {
            int t = disk_last_op == 'r' ? NEXT_TOKEN(disk_last_token) : FIRST_READ_TOKEN;
            if (t > valid_token) {
                break;
            }
            disk_last_token = t;
            if (disk_unit[disk_point] != 0 && objects[disk_unit[disk_point]].request_queue.size() > 0) {
                auto& object = objects[disk_unit[disk_point]];
                object.read(disk.id, disk_point, current_timestamp);
                active_object.push_back(object.id);
            }
            valid_token -= disk_last_token;
        } else {
            if (valid_token < 1) {
                break;
            }
            disk_last_token = FIRST_READ_TOKEN;
            valid_token -= 1;
        }
        disk_last_op = best_action[i];
        disk_point = FORWARD_STEP(disk_point);
        action_ss << best_action[i];
    }
    action_ss << "#\n";
    if (valid_token < 0) {
        std::cerr << valid_token << std::endl;
        std::cerr << best_action << std::endl;
    }
}

void Controller::delete_action_round2() {
    int num_delete;
    std::vector<int> delete_ids;
    std::vector<int> abort_ids;

    num_delete = delete_datas[current_timestamp].size();
    delete_ids.resize(num_delete);
    for (int i = 0; i < num_delete; i++) {
        delete_ids[i] = delete_datas[current_timestamp][i];
    }

    for (auto& delete_id : delete_ids) {
        auto& object = objects[delete_id];
        while (!object.request_queue.empty()) {
            auto request_id = object.request_queue.front();
            object.request_queue.pop();
            if (requests[request_id].is_done == false) {
                abort_ids.push_back(request_id);
            }
        }
        do_object_delete_round2(object);
        if (object.is_known) {
            stat_label_num[object.label] -= 1;
        }
    }

    printf("%ld\n", abort_ids.size());
    for (auto& abort_id : abort_ids) {
        printf("%d\n", abort_id);
    }
    fflush(stdout);
}

void Controller::write_action_round2() {
    int num_write;
    num_write = write_datas[current_timestamp].size();

    std::vector<std::vector<int>> write_ids(num_object_label + 1);
    for (int i = 0; i < num_write; i++) {
        int id, size, label;
        bool is_known = true;
        const auto& write_tuple = write_datas[current_timestamp][i];
        id = std::get<0>(write_tuple);
        size = std::get<1>(write_tuple);
        label = std::get<2>(write_tuple);
        if (object_label.count(id)) label = object_label[id];
        auto& object = objects[id];
        object.init(id, size, label, current_timestamp, is_known);
        write_ids[label].push_back(id);
        if (object.is_known) {
            stat_label_num[label] += 1;
        }
    }
    // std::shuffle(write_ids.begin(), write_ids.end(), std::default_random_engine(42));
    for (int i = 1; i <= num_object_label; i++) {
        int prv_disk = -1;
        for (auto& write_id : write_ids[i]) {
            auto& object = objects[write_id];
            do_object_write_round2(object, prv_disk);
            // prv_disk = object.replica[1];
            printf("%d\n", object.id);
            for (int j = 1; j <= REP_NUM; j++) {
                printf("%d", object.replica[j]);
                for (int k = 1; k <= object.size; k++) {
                    printf(" %d", object.units[j][k]);
                }
                printf("\n");
            }
        }
    }

    fflush(stdout);
}

void Controller::read_action_round2() {
    int num_read;
    int request_id, object_id;
    num_read = read_datas[current_timestamp].size();

    std::vector<int> label_read_num(num_object_label + 1, 0);
    std::vector<std::set<int>> label_object(num_object_label + 1);
    for (int i = 0; i < num_read; i++) {
        const auto& read_tuple = read_datas[current_timestamp][i];
        request_id = std::get<0>(read_tuple);
        object_id = std::get<1>(read_tuple);
        auto& request = requests[request_id];
        auto& object = objects[object_id];
        request.id = request_id;
        do_request_add_round2(request, object);
        if (object.is_known) {
            label_read_num[object.label] += 1;
            label_object[object.label].insert(object.id);
        } else {
            object.request_list.push_back(current_timestamp);
        }
    }
    for (int i = 1; i <= num_object_label; i++) {
        stat_label_read_num[current_timestamp][i] = label_object[i].empty() ? 0 : 1.0 * label_read_num[i] / stat_label_num[i];
    }

    std::vector<int> success_requests;
    std::vector<int> disk_permutation(num_disk);
    std::iota(disk_permutation.begin(), disk_permutation.end(), 1);
    // std::shuffle(disk_permutation.begin(), disk_permutation.end(), std::default_random_engine(42));
    std::vector<std::vector<std::string>> disk_action(num_disk + 1, std::vector<std::string>(HEAD_NUM + 1, ""));
    for (int j = 0; j < num_disk; j++) {
        for (int k = 1; k <= HEAD_NUM; k++) {
            auto i = disk_permutation[j];
            std::stringstream action_ss;
            do_disk_read_round2(disks2[i], k, action_ss);
            disk_action[i][k] = action_ss.str();
            for (auto& object_id : active_object) {
                auto& object = objects[object_id];
                if (object.is_delete) {
                    continue;
                }
                auto read_timestamp = object.get_last_all_read_timestamp();
                while (!object.request_queue.empty()) {
                    auto request_id = object.request_queue.front();
                    if (requests[request_id].is_done == false) {
                        if (read_timestamp >= requests[request_id].start_timestamp) {
                            success_requests.push_back(request_id);
                            requests[request_id].is_done = true;
                        } else {
                            break;
                        }
                    } else {
                        // std::cerr << "Request is done" << std::endl;
                        // assert(false && "Request is done");
                    }
                    object.request_queue.pop();
                }
            }
            active_object.clear();
        }
    }

    for (int i = 1; i <= num_disk; i++) {
        for (int j = 1; j <= HEAD_NUM; j++) {
            printf("%s", disk_action[i][j].c_str());
        }
    }

    printf("%ld\n", success_requests.size());
    for (auto& request_id : success_requests) {
        printf("%d\n", request_id);
    }

    // 计算繁忙的请求
    while (!delete_queue.empty()) {
        auto request_id = delete_queue.front();
        if (requests[request_id].is_done || objects[requests[request_id].object_id].is_delete) {
            delete_queue.pop();
            continue;
        }
        if (requests[request_id].start_timestamp + EXTRA_TIME > current_timestamp) {
            break;
        }
        busy_requests.push_back(request_id);
        requests[request_id].is_done = true;
        delete_queue.pop();
    }

    printf("%ld\n", busy_requests.size());
    for (auto& request_id : busy_requests) {
        printf("%d\n", request_id);
    }
    busy_requests.clear();

    fflush(stdout);
}

void Controller::gc_action_round2() {
    printf("GARBAGE COLLECTION\n");
    std::vector<int> label_permutation(num_object_label);
    std::iota(label_permutation.begin(), label_permutation.end(), 1);
    for (int i = 1; i <= num_disk; i++) {
        std::vector<std::pair<int, int>> swap_ops;
        disks2[i].do_gc(swap_ops, objects, max_swap_num, label_permutation);
        printf("%ld\n", swap_ops.size());
        for (auto& swap_op : swap_ops) {
            printf("%d %d\n", swap_op.first, swap_op.second);
        }
    }
    fflush(stdout);
}
