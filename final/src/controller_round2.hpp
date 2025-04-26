#pragma once
#include "controller.hpp"

void Controller::incre_action() {
    int num_incre;
    int obj_id, obj_label;
    scanf("%d", &num_incre);
    int corret_num = 0, total_num = 0;
    for (int i = 1; i <= num_incre; ++i) {
        scanf("%d%d", &obj_id, &obj_label);
        if (object_label.count(obj_id)) {
            if (object_label[obj_id] == obj_label) {
                corret_num ++;
            }
            total_num ++;
        }
        object_label[obj_id] = obj_label;
    }
    std::cerr << "CORRECT NUM: " << corret_num << "(" << 100.0 * corret_num / num_incre << "%)"<< std::endl;
    // if (1.0 * corret_num / total_num < 0.9) {
    //     printf("no!");
    // }
}

void Controller::start_round2() {
    incre_action();
    current_round = 2;

    // 统计一些信息
    std::vector<std::vector<int>> stat_label_write_units(num_timestamp + 1, std::vector<int>(num_object_label + 1, 0));
    std::vector<std::vector<int>> stat_label_delete_units(num_timestamp + 1, std::vector<int>(num_object_label + 1, 0));
    // std::ofstream file("delete_time.txt");
    // if (!file.is_open()) {
    //     std::cerr << "Failed to open ground truth file" << std::endl;
    //     return;
    // }
    for (int i = 1; i <= num_timestamp; i ++) {
        for (auto& obj_id : delete_datas[i]) {
            auto& object = objects[obj_id];
            stat_label_delete_units[i][object.label] += object.size;
        }
        for (auto& [obj_id, _, __] : write_datas[i]) {
            auto& object = objects[obj_id];
            int label = object.is_known ? object.label : (object_label.count(obj_id) ? object_label[obj_id] : 0);
            if(object.request_list.size() < 50) label = 0; //  || (object.delete_timestamp != -1 && object.delete_timestamp - object.write_timestamp <= 1800 * 10)
            object.label = label;
            stat_label_write_units[i][label] += object.size;
            // file << label << " " << (object.delete_timestamp == -1 ? num_timestamp : object.delete_timestamp) - object.write_timestamp << std::endl;
        }
    }
    // file.close();

    int stat_slice_num = GET_STAT_FREQ(num_timestamp);
    freq_read.resize(num_object_label + 1, std::vector<double>(stat_slice_num + 1, 0));
    freq_read_slice.resize(num_object_label + 1, std::vector<double>(num_slice + 1, 0));
    std::vector<std::vector<int>> freq_write(num_object_label + 1, std::vector<int>(stat_slice_num + 1, 0));
    std::vector<std::vector<int>> freq_delete(num_object_label + 1, std::vector<int>(stat_slice_num + 1, 0));
    std::vector<int> label_max_num(num_object_label + 1, 0);
    std::vector<int> label_write_num(num_object_label + 1, 0);
    std::vector<int> label_delete_num(num_object_label + 1, 0);
    std::vector<long long> label_sum_num(num_object_label + 1, 0);
    std::vector<int> label_avg_num(num_object_label + 1, 0);
    for (int i = 1; i <= num_timestamp; i ++) {
        for (int j = 0; j <= num_object_label; j++) {
            freq_read[j][GET_STAT_FREQ(i)] += stat_label_read_num[i][j];
            freq_read_slice[j][GET_FREQ(i)] += stat_label_read_num[i][j];
            freq_write[j][GET_STAT_FREQ(i)] += stat_label_write_units[i][j];
            freq_delete[j][GET_STAT_FREQ(i)] += stat_label_delete_units[i][j];
            freq_read_slice[j][GET_FREQ(i)] += stat_label_read_num[i][j];
            label_write_num[j] += stat_label_write_units[i][j];
            label_delete_num[j] += stat_label_delete_units[i][j];
            label_max_num[j] = std::max(label_max_num[j], label_write_num[j] - label_delete_num[j]);
            label_sum_num[j] += label_write_num[j] - label_delete_num[j];
        }
    }
    // [TODO] 调整为最大值、平均值、中位数、75分位数
    for (int i = 0; i <= num_object_label; i++) {
        label_avg_num[i] = label_sum_num[i] / num_timestamp;
        std::cerr << label_avg_num[i] << " ";
    }
    std::cerr << std::endl;
#ifdef DEBUG
    std::ofstream file("stat.txt");
    if (!file.is_open()) {
        std::cerr << "Failed to open ground truth file" << std::endl;
        return;
    }
    for (int i = 1; i <= num_object_label; i ++) {
        for (int j = 1; j <= stat_slice_num; j++) {
            file << freq_read[i][j] << " ";
        }
        file << std::endl;
    }
    file.close();
#endif

    // 划分8+8
    std::vector<int> freq_read_avg(std::vector<int>(stat_slice_num + 1, 0));
    for (int i = 1; i <= num_object_label; i++) {
        for (int j = 1; j <= stat_slice_num; j++) {
            freq_read_avg[j] += freq_read[i][j];
        }
    }
    for (int j = 1; j <= stat_slice_num; j++) {
        freq_read_avg[j] /= num_object_label;
    }
    // 从1-16个label中选出8个，计算它们的平均read与freq_read_avg的L2 loss，选出最小loss的八个label
    std::vector<int> best_labels(8, 0);
    double min_loss = std::numeric_limits<double>::max();

    // Try all combinations of 8 labels using a recursive approach
    std::vector<int> current_labels;
    std::function<void(int, int)> generate_combinations;
    
    generate_combinations = [&](int idx, int count) {
        // Base case: if we've selected 8 labels
        if (count == 8) {
            // Calculate average read frequency for selected 8 labels
            std::vector<double> selected_avg(stat_slice_num + 1, 0);
            
            for (int label : current_labels) {
                for (int j = 1; j <= stat_slice_num; j++) {
                    selected_avg[j] += freq_read[label][j];
                }
            }
            
            // Normalize to get average
            for (int j = 1; j <= stat_slice_num; j++) {
                selected_avg[j] /= 8;
            }
            
            // Calculate L2 loss
            double loss = 0.0;
            for (int j = 1; j <= stat_slice_num; j++) {
                loss += std::pow(selected_avg[j] - freq_read_avg[j], 2);
            }
            
            // Update best labels if loss is smaller
            if (loss < min_loss) {
                min_loss = loss;
                for (int i = 0; i < 8; i++) {
                    best_labels[i] = current_labels[i];
                }
            }
            return;
        }
        
        // Base case: not enough labels left to select
        if (idx > num_object_label) {
            return;
        }
        
        // Include current index
        current_labels.push_back(idx);
        generate_combinations(idx + 1, count + 1);
        current_labels.pop_back();
        
        // Exclude current index
        generate_combinations(idx + 1, count);
    };
    
    // Start combination generation from index 1 with 0 labels selected
    generate_combinations(1, 0);

    std::cerr << "Best 8 labels with min L2 loss: ";
    for (int label : best_labels) {
        std::cerr << label << " ";
    }
    std::cerr << std::endl;
    std::cerr << "Min L2 loss: " << min_loss << std::endl;

    // 归一化处理
    std::vector<std::vector<double>> normalized_freq_read(num_object_label + 1, std::vector<double>(stat_slice_num + 1, 0.0));
    for (int i = 1; i <= num_object_label; i++) {
        int max_freq = *std::max_element(freq_read[i].begin() + 1, freq_read[i].end());
        for (int j = 1; j <= stat_slice_num; j++) {
            normalized_freq_read[i][j] = freq_read[i][j] / max_freq;
            // [TODO] 可以不01
            normalized_freq_read[i][j] = normalized_freq_read[i][j] < 0.1 ? 0 : 1;
        }
    }

    // 计算距离矩阵
    std::vector<std::vector<double>> distance(num_object_label + 1, std::vector<double>(num_object_label + 1, 0.0));
    for (int i = 1; i <= num_object_label; i++) {
        for (int j = 1; j <= num_object_label; j++) {
            double dist = 0.0;
            for (int l = 1; l <= stat_slice_num; l++) {
                dist += std::pow(normalized_freq_read[i][l] - normalized_freq_read[j][l], 2);
            }
            distance[i][j] = std::sqrt(dist);
        }
    }
    // Use best_labels as the first 8 labels, then the remaining 8, and 0 as the last
    std::vector<int> label_permutation(num_object_label + 1, 0);
    // Create a set of best labels for O(1) lookup
    std::unordered_set<int> best_labels_set(best_labels.begin(), best_labels.end());
    
    // Prepare lists for best labels and remaining labels
    std::vector<int> best_list, remaining_list;
    for (int i = 1; i <= num_object_label; i++) {
        if (best_labels_set.count(i)) {
            best_list.push_back(i);
        } else {
            remaining_list.push_back(i);
        }
    }
    
    // Apply the greedy nearest neighbor algorithm for best_list
    std::vector<int> ordered_best;
    if (!best_list.empty()) {
        std::vector<bool> visited(num_object_label + 1, false);
        // [TODO] 调参！
        int current = best_list[2];
        visited[current] = true;
        ordered_best.push_back(current);
        
        for (int i = 1; i < best_list.size(); i++) {
            double min_dist = std::numeric_limits<double>::max();
            int next_node = -1;
            for (int j : best_list) {
                if (!visited[j] && distance[current][j] < min_dist) {
                    min_dist = distance[current][j];
                    next_node = j;
                }
            }
            visited[next_node] = true;
            ordered_best.push_back(next_node);
            current = next_node;
        }
    }
    
    // Apply the greedy nearest neighbor algorithm for remaining_list
    std::vector<int> ordered_remaining;
    if (!remaining_list.empty()) {
        std::vector<bool> visited(num_object_label + 1, false);
        // [TODO] 调参！
        int current = remaining_list[2];
        visited[current] = true;
        ordered_remaining.push_back(current);
        
        for (int i = 1; i < remaining_list.size(); i++) {
            double min_dist = std::numeric_limits<double>::max();
            int next_node = -1;
            for (int j : remaining_list) {
                if (!visited[j] && distance[current][j] < min_dist) {
                    min_dist = distance[current][j];
                    next_node = j;
                }
            }
            visited[next_node] = true;
            ordered_remaining.push_back(next_node);
            current = next_node;
        }
    }
    
    // Combine best_labels first, then remaining labels, with 0 at index 0
    label_permutation[0] = 0;
    for (int i = 0; i < ordered_best.size(); i++) {
        label_permutation[i + 1] = ordered_best[i];
        std::cerr << ordered_best[i] << " ";
    }
    for (int i = 0; i < ordered_remaining.size(); i++) {
        label_permutation[i + ordered_best.size() + 1] = ordered_remaining[i];
        std::cerr << ordered_remaining[i] << " ";
    }
    std::cerr << std::endl;

    // == 计算磁盘中label需要的区域大小
    std::vector<std::vector<int>> disk_label_num(num_disk + 1, std::vector<int>(num_object_label + 1, 0));
    gen = std::mt19937(SEED);
    std::uniform_real_distribution<> dis(0.6, 1.4);
    for (int i = 1; i <= num_disk; i++) {
        for (int j = 0; j <= num_object_label; j++) {
            disk_label_num[i][j] = label_avg_num[j] * dis(gen);
        }
    }
    
    disks2.resize(num_disk + 1);
    std::vector<double> label_num_norm(num_object_label + 1, 0.0);
    for (int i = 1; i <= num_disk; i++) {
        int total_label_object_max_num = std::accumulate(disk_label_num[i].begin(), disk_label_num[i].end(), 0);
        for (int j = 0; j <= num_object_label; j++) {
            label_num_norm[j] = 1.0 * disk_label_num[i][j] / total_label_object_max_num;
        }
        disks2[i].init2(i, num_disk_unit, num_object_label, label_num_norm, label_permutation);
    }
    // 下面reset round2也需要用到的变量
    while (!delete_queue.empty()) {
        delete_queue.pop();
    }
    for (int i = 1; i <= num_disk; i++) {
        for (int j = 1; j <= HEAD_NUM; j++) {
            stat_read_speed[i][j].clear();
        }
    }
    for (int i = 1; i <= num_disk; i++) {
        for (int j = 1; j <= HEAD_NUM; j++) {
            stat_obj_num[i][j] = 0;
        }
    }
    all_score = read_score = busy_score = 0;
}

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
    for (int i = 1; i <= REP_NUM; i++) {
        auto& disk = disks2[(min_value_disk + i - 2) % num_disk + 1];
        disk.add_object(object, min_value_units, i, min_value_head, -1);
    }
}

void Controller::do_request_add_round2(Request& request, Object& object) {
    all_score += G(object.size);
    request.object_id = object.id;
    request.start_timestamp = current_timestamp;
    assert(object.request_list[object.request_list_idx] == current_timestamp);
    object.request_list_idx ++;
    // if (current_slice > 10 && object.request_list_idx < object.request_list.size() && object.request_list[object.request_list_idx] - current_timestamp > 1000) {
    //     request.is_done = true;
    //     busy_requests.push_back(request.id);
    //     return;
    // }
    if (not_considered_label.count(object.label) || abort_requests.count(request.id)) {
        request.is_done = true;
        busy_requests.push_back(request.id);
        return;
    }
    // if (current_slice > 10 && object.label == 0) {
    //     request.is_done = true;
    //     busy_requests.push_back(request.id);
    //     return;
    // }
    // if (current_slice > 10 && freq_read[object.label][GET_STAT_FREQ(current_timestamp)] < 1) {
    //     request.is_done = true;
    //     busy_requests.push_back(request.id);
    //     return;
    // }
    request.is_done = false;
    object.request_queue.push(request.id);
    delete_queue.push(request.id);
}

void Controller::do_disk_read_round2(DiskRound2& disk, int head_id, std::stringstream& action_ss) {
    auto& disk_unit = disk.units;
    auto& disk_point = disk.heads[head_id].point;
    auto& disk_last_op = disk.heads[head_id].last_op;
    auto& disk_last_token = disk.heads[head_id].last_token;

    // 判断是不是该跳转了
    int step = disk.get_next_read_step(head_id, objects, 0);
    if (step >= max_num_token && disk_point + step <= disk.head_regions[head_id].end_point) {
        action_ss << "j " << disk_point + step << "\n";
        disk_point = disk_point + step;
        disk_last_op = 'j';
        disk_last_token = FIRST_READ_TOKEN;
        return;
    } else if (step == -1) {
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
                    score += 1.0 * objects[disk_unit[current_point]].request_queue.size();
                }
            }
            // if (last_op == 'r') {
            //     score += (1 - 1.0 * last_token / FIRST_READ_TOKEN) * 0.5;
            // }
            return score;
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
                return calc_rp(current_point, current_token, 'p', FIRST_READ_TOKEN, true, action);
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
                double pass_score = calc_rp(temp_point, temp_token, 'p', FIRST_READ_TOKEN, true, temp_action);
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
                double read_score = calc_rp(current_point, flag ? 0 : current_token, last_op, last_token, true, read_action);
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
            return calc_rp(current_point, flag ? 0 : current_token, last_op, last_token, false, action);
        }
    };
    std::string best_action;
    bool last_empty_ = disk_unit[BACKWARD_STEP(disk_point)] == 0 || objects[disk_unit[BACKWARD_STEP(disk_point)]].request_queue.size() == 0;
    double rp_score = calc_rp(disk_point, max_num_token * TOKEN_CONSIDERED_RATE, disk_last_op, disk_last_token, last_empty_, best_action);

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
    }

    ss_round2 << abort_ids.size() << "\n";
    for (auto& abort_id : abort_ids) {
        ss_round2 << abort_id << "\n";
    }
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
        auto& object = objects[id];
        label = object.label;
        object.init(id, size, label, current_timestamp, is_known);
        write_ids[label].push_back(id);
    }
    // std::shuffle(write_ids.begin(), write_ids.end(), std::default_random_engine(42));
    for (int i = 0; i <= num_object_label; i++) {
        int prv_disk = -1;
        for (auto& write_id : write_ids[i]) {
            auto& object = objects[write_id];
            do_object_write_round2(object, prv_disk);
            // prv_disk = object.replica[1];
            ss_round2 << object.id << "\n";
            for (int j = 1; j <= REP_NUM; j++) {
                ss_round2 << object.replica[j];
                for (int k = 1; k <= object.size; k++) {
                    ss_round2 << " " << object.units[j][k];
                }
                ss_round2 << "\n";
            }
        }
    }

    fflush(stdout);
}

void Controller::read_action_round2() {
    int num_read;
    int request_id, object_id;
    num_read = read_datas[current_timestamp].size();

    for (int i = 0; i < num_read; i++) {
        const auto& read_tuple = read_datas[current_timestamp][i];
        request_id = std::get<0>(read_tuple);
        object_id = std::get<1>(read_tuple);
        auto& request = requests[request_id];
        auto& object = objects[object_id];
        request.id = request_id;
        do_request_add_round2(request, object);
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

    // 磁头运动
    for (int i = 1; i <= num_disk; i++) {
        for (int j = 1; j <= HEAD_NUM; j++) {
            ss_round2 << disk_action[i][j];
        }
    }

    // 成功的请求
    ss_round2 << success_requests.size() << "\n";
    for (auto& request_id : success_requests) {
        auto& object = objects[requests[request_id].object_id];
        auto& request = requests[request_id];
        read_score += F(current_timestamp - request.start_timestamp) * G(object.size);
        ss_round2 << request_id << "\n";
    }

    // 处理超时的请求
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
        abort_requests.insert(request_id);
    }

    // 返回繁忙的请求
    ss_round2 << busy_requests.size() << "\n";
    for (auto& request_id : busy_requests) {
        auto& object = objects[requests[request_id].object_id];
        auto& request = requests[request_id];
        busy_score += (current_timestamp - request.start_timestamp) / 105.0 * G(object.size);
        ss_round2 << request_id << "\n";
    }
    busy_requests.clear();

    fflush(stdout);
}

void Controller::gc_action_round2() {
    ss_round2 << "GARBAGE COLLECTION\n";
    std::vector<int> label_permutation(num_object_label);
    std::iota(label_permutation.begin(), label_permutation.end(), 1);
    int next_slice = current_slice == num_slice ? current_slice : current_slice + 1;
    std::sort(label_permutation.begin(), label_permutation.end(), [&](int a, int b) {
        return freq_read_slice[a][next_slice] > freq_read_slice[b][next_slice];
    });
    for (int i = 1; i <= num_disk; i++) {
        std::vector<std::pair<int, int>> swap_ops;
        disks2[i].do_gc(swap_ops, objects, max_swap_num, label_permutation);
        ss_round2 << swap_ops.size() << "\n";
        for (auto& swap_op : swap_ops) {
            ss_round2 << swap_op.first << " " << swap_op.second << "\n";
        }
    }
    fflush(stdout);
}

void Controller::update_round2() {
    // 更新磁盘状态
    for (int i = 1; i <= num_disk; i++) {
        auto& disk = disks2[i];
        double freq_read_sum = 0;
        for (int j = disk.write_region.start_point; j <= disk.write_region.end_point; j ++) {
            if (disk.units[j] == 0) continue;
            if (not_considered_label.count(objects[disk.units[j]].label)) continue;
            freq_read_sum += freq_read_slice[objects[disk.units[j]].label][GET_FREQ(current_timestamp)];
        }
        double freq_read_half = 0;
        int divide_point = 0;
        for (int j = disk.write_region.start_point; j <= disk.write_region.end_point; j ++) {
            if (disk.units[j] == 0) continue;
            if (not_considered_label.count(objects[disk.units[j]].label)) continue;
            freq_read_half += freq_read_slice[objects[disk.units[j]].label][GET_FREQ(current_timestamp)];
            if (freq_read_half >= freq_read_sum / 2) {
                divide_point = j;
                break;
            }
        }
        // for (int i = 1; i <= num_object_label; i ++) {
        //     if (disk.write_region.label_points[i].start_point <= divide_point && disk.write_region.label_points[i].end_point >= divide_point) {
        //         if (divide_point - disk.write_region.label_points[i].start_point > disk.write_region.label_points[i].end_point - divide_point) {
        //             divide_point = disk.write_region.label_points[i].end_point;
        //         } else {
        //             divide_point = disk.write_region.label_points[i].start_point - 1;
        //         }
        //     }
        // }
        disk.head_regions[1].end_point = divide_point;
        disk.head_regions[2].start_point = divide_point + 1;
    }
}

void Controller::simulate_round2_once() {
    int saved_timestamp = current_timestamp;
    int saved_slice = current_slice;
    
    // Backup variables that will be modified
    std::vector<DiskRound2> saved_disks2 = disks2;
    std::queue<int> saved_delete_queue = delete_queue;
    std::vector<std::vector<std::vector<int>>> saved_stat_read_speed = stat_read_speed;
    double saved_all_score = all_score;
    double saved_read_score = read_score;
    double saved_busy_score = busy_score;
    
    // Copy only existing requests and objects to save memory
    std::vector<Request> saved_requests;
    std::set<int> saved_requests_set;
    std::vector<Object> saved_objects;
    std::set<int> saved_objects_set;
    // for (auto& request : requests) {
    //     if (request.id > 0) {
    //         saved_requests.push_back(request);
    //         saved_requests_set.insert(request.id);
    //     }
    // }
    // for (auto& object : objects) {
    //     if (object.id > 0) {
    //         saved_objects.push_back(object);
    //         saved_objects_set.insert(object.id);
    //     }
    // }
    update_round2();
    for (int i = saved_timestamp - 105; i <= saved_timestamp + FRE_PER_SLICING; i ++) {
        if (i > num_timestamp) break;
        for (auto& delete_id : delete_datas[i]) {
            if (saved_objects_set.count(delete_id)) continue;
            saved_objects_set.insert(delete_id);
            auto& object = objects[delete_id];
            saved_objects.push_back(object);
        }
        for (auto& write_tuple : write_datas[i]) {
            int id = std::get<0>(write_tuple);
            if (saved_objects_set.count(id)) continue;
            saved_objects_set.insert(id);
            auto& object = objects[id];
            saved_objects.push_back(object);
        }
        for (auto& read_tuple : read_datas[i]) {
            int request_id = std::get<0>(read_tuple);
            int object_id = std::get<1>(read_tuple);
            if (saved_requests_set.count(request_id)) continue;
            saved_requests_set.insert(request_id);
            auto& request = requests[request_id];
            saved_requests.push_back(request);
            if (saved_objects_set.count(object_id)) continue;
            saved_objects_set.insert(object_id);
            auto& object = objects[object_id];
            saved_objects.push_back(object);
        }
    }
    
    // Simulate
    once_score = 0;
    for (int i = saved_timestamp + 1; i <= saved_timestamp + FRE_PER_SLICING; i ++) {
        if (i > num_timestamp) break;
        current_timestamp = i;
        current_slice = GET_FREQ(current_timestamp);
        delete_action_round2();
        write_action_round2();
        read_action_round2();
    }
    once_score = read_score;
    
    // Restore variables that were modified
    disks2 = saved_disks2;
    delete_queue = saved_delete_queue;
    stat_read_speed = saved_stat_read_speed;
    all_score = saved_all_score;
    read_score = saved_read_score;
    busy_score = saved_busy_score;
    
    // Restore requests and objects
    for (auto& request : saved_requests) {
        requests[request.id] = request;
    }
    for (auto& object : saved_objects) {
        objects[object.id] = object;
    }
    // Restore current timestamp and slice
    
    current_timestamp = saved_timestamp;
    current_slice = saved_slice;
    ss_round2.str("");
    ss_round2.clear();
}

void Controller::simulate_round2() {
    // if (current_slice < 7) return ;
    not_considered_label.clear();
    abort_requests.clear();
    int next_slice = current_slice == num_slice ? current_slice : current_slice + 1;
    std::vector<int> label_permutation(num_object_label);
    std::iota(label_permutation.begin(), label_permutation.end(), 1);
    std::sort(label_permutation.begin(), label_permutation.end(), [&](int a, int b) {
        return freq_read_slice[a][next_slice] < freq_read_slice[b][next_slice];
    });
    std::vector<int> temp(1, 0);
    for (int i = 0; i < num_object_label; i++) {
        temp.push_back(label_permutation[i]);
    }
    label_permutation = temp;
    double best_score = 0;
    once_score = 0;
    std::set<int> best_set;
    std::set<int> best_abort_requests;
    for (int i = 0; i < num_object_label; i++) {
        // if (i > 5) {
            simulate_round2_once();
        // }
        // std::cerr << once_score << std::endl;
        if (once_score > best_score) {
            best_score = once_score;
            best_set = not_considered_label;
            best_abort_requests = abort_requests;
        }
        not_considered_label.insert(label_permutation[i]);
        abort_requests.clear();
    }
    not_considered_label = best_set;
    abort_requests = best_abort_requests;
}
