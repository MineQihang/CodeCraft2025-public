#pragma once
#include "disk.hpp"
#include "object.hpp"
#include "request.hpp"
#include "utils.hpp"

class Controller {
   private:
    // 输入
    int num_timestamp;                          // 时间片数量, T in [1+105, 86400+105]
    int num_object_label;                       // 对象标签数量, M in [1, 16]
    int num_disk;                               // 磁盘数量, N in [3, 10]
    int num_disk_unit;                          // 磁盘单元数量, V in [1, 16384]
    int max_num_token;                          // 最大令牌数量, G in [64, 500]
    int max_swap_num;                           // 每个硬盘最多的交换存储单元的操作次数, K in [0, 100]
    std::vector<std::vector<int>> freq_delete;  // 删除操作频率
    std::vector<std::vector<int>> freq_write;   // 写操作频率
    std::vector<std::vector<int>> freq_read;    // 读操作频率
    int num_slice;                              // 频率数量
    std::vector<int> extra_token;               // 额外的令牌数量
    int num_extra_token;                        // 额外的令牌数量
    int num_token;

    // 主要数据
    std::vector<Disk> disks;
    Request requests[MAX_REQUEST_NUM];
    Object objects[MAX_OBJECT_NUM];

    // 中间变量
    int current_timestamp;                            // 当前时间戳
    int current_slice;                                // 当前时间片
    std::vector<int> active_object;                   // 当前被read的对象
    std::set<int> ignored_obj;                        // 被忽略的对象
    std::mt19937 gen;                                 // 随机数生成器
    std::queue<int> delete_queue;                     // 删除队列
    std::vector<std::set<int>> not_considered_label;  // 每个时间片不考虑的请求label
    std::vector<int> busy_requests;                   // 繁忙的请求
    std::vector<std::vector<int>> cumsum_delete;
    std::vector<std::vector<int>> cumsum_write;
    std::vector<std::vector<int>> cumsum_read;
    std::vector<int> freq_read_online;  // 在线读频率

    // 统计变量
    std::vector<std::vector<std::vector<int>>> stat_read_speed;  // 读速度统计
    std::vector<std::vector<int>> stat_obj_num;                  // 对象数量统计

    // 操作函数
    void timestamp_action();
    void do_object_delete(Object& object);
    void delete_action();
    void do_object_write(Object& object, int prv_disk);
    void write_action();
    void do_request_add(Request& request, Object& object);
    void read_action_for_disk(Disk& disk, int head_id, std::stringstream& action_ss);
    void read_action();
    void gc_action();

   public:
    // 外部函数
    void init();
    void interact();
    void print_stat_info();
};

void Controller::timestamp_action() {
    int timestamp;
    scanf("%*s%d", &timestamp);

    current_slice = GET_FREQ(timestamp);
    num_token = extra_token[int(ceil(timestamp / 1800.0))] + max_num_token;
    assert(timestamp == current_timestamp && "Timestamp error");

    printf("TIMESTAMP %d\n", timestamp);
    fflush(stdout);
}

void Controller::do_object_delete(Object& object) {
    for (int i = 1; i <= REP_NUM; i++) {
        Disk& disk = disks[object.replica[i]];
        disk.delete_units(object.units[i]);
        if (i == 1) stat_obj_num[disk.id][object.units[i][1] < disk.regions[1][1].end_point ? 1 : 2] -= 1;
    }
    object.delete_();
}

void Controller::delete_action() {
    int num_delete;
    std::vector<int> delete_ids;
    std::vector<int> abort_ids;

    scanf("%d", &num_delete);
    delete_ids.resize(num_delete);
    for (int i = 0; i < num_delete; i++) {
        scanf("%d", &delete_ids[i]);
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
        do_object_delete(object);
    }

    printf("%d\n", abort_ids.size());
    for (auto& abort_id : abort_ids) {
        printf("%d\n", abort_id);
    }
    fflush(stdout);
}

void Controller::do_object_write(Object& object, int prv_disk = -1) {
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
            auto& disk = disks[i];
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
    int start_point = disks[min_value_disk].regions[1][min_value_head].start_point;
    for (int i = 1; i <= REP_NUM; i++) {
        auto& disk = disks[(min_value_disk + i - 2) % num_disk + 1];
        disk.add_object(object, min_value_units, i, min_value_head, start_point);
        if (i == 1) stat_obj_num[min_value_disk][min_value_head] += 1;
    }
}

void Controller::write_action() {
    int num_write;
    scanf("%d", &num_write);
    std::vector<std::vector<int>> write_ids(num_object_label + 1);
    for (int i = 1; i <= num_write; i++) {
        int id, size, label;
        scanf("%d%d%d", &id, &size, &label);
        auto& object = objects[id];
        object.init(id, size, label);
        write_ids[label].push_back(id);
    }
    // std::shuffle(write_ids.begin(), write_ids.end(), std::default_random_engine(42));
    for (int i = 1; i <= num_object_label; i++) {
        int prv_disk = -1;
        for (auto& write_id : write_ids[i]) {
            auto& object = objects[write_id];
            do_object_write(object, prv_disk);
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

void Controller::do_request_add(Request& request, Object& object) {
    request.object_id = object.id;
    request.start_timestamp = current_timestamp;
    int last_read = cumsum_read[object.label][current_slice] - freq_read_online[object.label];
    int last_obj = cumsum_write[object.label][current_slice] - cumsum_delete[object.label][current_slice];
    int last_timestamp = current_slice * FRE_PER_SLICING - current_timestamp;
    auto disk_id = object.replica[1];
    auto head_id = object.units[1][1] > disks[object.replica[1]].regions[1][2].start_point ? 2 : 1;
    auto& srs = stat_read_speed[disk_id][head_id];
    int read_speed = srs.size() >= 5 ? (srs[srs.size() - 1] - srs[srs.size() - 5]) / 4 : 0;
    // std::cerr << last_timestamp << std::endl;
    double value = 1.0 * last_read / (last_obj + 1) / (last_timestamp + 1) / (read_speed + 1);
    // std::cerr << value << std::endl;
    // if(object.label == 1) std::cerr << last_read << " " << last_obj << " " << last_timestamp << std::endl;
    freq_read_online[object.label] += object.size;
    if (object.request_queue.size() == 0 && current_slice > 5 && value < 0.00018) { //not_considered_label[current_slice].count(object.label)) { // current_slice > 10 && ((current_slice <= 15 && value < 0.0003) || (current_slice > 15 && value < 0.0002))
        if ((current_slice >= 34 && object.label == 2)) {
            request.is_done = false;
            object.request_queue.push(request.id);
            delete_queue.push(request.id);
            return ;
        }
        request.is_done = true;
        busy_requests.push_back(request.id);
    } 
    // else if ((current_slice == 42 || current_slice == 41) && object.label != 2) {
    //     request.is_done = true;
    //     busy_requests.push_back(request.id);
    // }
    else {
        request.is_done = false;
        object.request_queue.push(request.id);
        delete_queue.push(request.id);
    }
}

void Controller::read_action_for_disk(Disk& disk, int head_id, std::stringstream& action_ss) {
    auto& disk_unit = disk.units;
    auto& disk_point = disk.heads[head_id].point;
    auto& disk_last_op = disk.heads[head_id].last_op;
    auto& disk_last_token = disk.heads[head_id].last_token;

    // 判断是不是该跳转了
    int step = disk.get_next_read_step(head_id, objects);
    if (step >= num_token && disk_point + step <= disk.regions[1][head_id].end_point) {
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
    calc_rp(disk_point, num_token * TOKEN_CONSIDERED_RATE, disk_last_op, disk_last_token, last_empty_, best_action);

    // 根据action_ss更新状态
    int valid_token = num_token;
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

void Controller::read_action() {
    int num_read;
    int request_id, object_id;
    scanf("%d", &num_read);
    for (int i = 1; i <= num_read; i++) {
        scanf("%d%d", &request_id, &object_id);
        auto& request = requests[request_id];
        request.id = request_id;
        do_request_add(requests[request_id], objects[object_id]);
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
            read_action_for_disk(disks[i], k, action_ss);
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

    printf("%d\n", success_requests.size());
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

    printf("%d\n", busy_requests.size());
    for (auto& request_id : busy_requests) {
        printf("%d\n", request_id);
    }
    busy_requests.clear();

    fflush(stdout);
}

void Controller::gc_action() {
    scanf("%*s %*s");
    printf("GARBAGE COLLECTION\n");
    std::vector<int> label_permutation(num_object_label);
    std::iota(label_permutation.begin(), label_permutation.end(), 1);
    int next_slice = current_slice + int(current_slice < num_slice);
    std::sort(label_permutation.begin(), label_permutation.end(),
              [&](int a, int b) {
                  return (freq_read[a][next_slice]) / (cumsum_write[a][next_slice] - cumsum_delete[a][next_slice] + 1) > (freq_read[b][next_slice]) / (cumsum_write[b][next_slice] - cumsum_delete[b][next_slice] + 1);
              });
    for (int i = 1; i <= num_disk; i++) {
        std::vector<std::pair<int, int>> swap_ops;
        disks[i].do_gc(swap_ops, objects, max_swap_num, label_permutation);
        printf("%d\n", swap_ops.size());
        for (auto& swap_op : swap_ops) {
            printf("%d %d\n", swap_op.first, swap_op.second);
        }
    }
    fflush(stdout);
}

void Controller::init() {
    // == 输入基本信息
    scanf("%d%d%d%d%d%d", &num_timestamp, &num_object_label, &num_disk,
          &num_disk_unit, &max_num_token, &max_swap_num);

    // == 输入对象label的特征
    freq_delete.resize(num_object_label + 1);
    freq_write.resize(num_object_label + 1);
    freq_read.resize(num_object_label + 1);
    num_slice = GET_FREQ(num_timestamp);
    cumsum_delete.resize(num_object_label + 1);
    cumsum_write.resize(num_object_label + 1);
    cumsum_read.resize(num_object_label + 1);
    freq_read_online.resize(num_object_label + 1, 0);
    for (int i = 1; i <= num_object_label; i++) {
        freq_delete[i].resize(num_slice + 1);
        cumsum_delete[i].resize(num_slice + 1, 0);
        for (int j = 1; j <= num_slice; j++) {
            scanf("%d", &freq_delete[i][j]);
            cumsum_delete[i][j] = cumsum_delete[i][j - 1] + freq_delete[i][j];
        }
    }
    for (int i = 1; i <= num_object_label; i++) {
        freq_write[i].resize(num_slice + 1);
        cumsum_write[i].resize(num_slice + 1, 0);
        for (int j = 1; j <= num_slice; j++) {
            scanf("%d", &freq_write[i][j]);
            cumsum_write[i][j] = cumsum_write[i][j - 1] + freq_write[i][j];
        }
    }
    int max_read = 0;
    for (int i = 1; i <= num_object_label; i++) {
        freq_read[i].resize(num_slice + 2, 0);
        cumsum_read[i].resize(num_slice + 1, 0);
        for (int j = 1; j <= num_slice; j++) {
            scanf("%d", &freq_read[i][j]);
            cumsum_read[i][j] = cumsum_read[i][j - 1] + freq_read[i][j];
            max_read = std::max(max_read, freq_read[i][j]);
        }
    }

    num_extra_token = ceil(1.0 * (num_timestamp + 105) / FRE_PER_SLICING);
    extra_token.resize(num_extra_token + 1, 0);
    for (int i = 1; i <= num_extra_token; i++) {
        scanf("%d", &extra_token[i]);
    }

    // == 计算每个时间片不需要考虑的label
    not_considered_label.resize(num_slice + 2);
    for (int i = 10; i <= num_slice; i++) {
        std::vector<std::pair<int, double>> temp;
        for (int j = 1; j <= num_object_label; j++) {
            temp.push_back(std::make_pair(j, 1.0 * freq_read[j][i] / (cumsum_write[j][i] - cumsum_delete[j][i] + 1)));
        }
        std::sort(temp.begin(), temp.end(),
                  [](const std::pair<int, double>& a, const std::pair<int, double>& b) {
                      return a.second < b.second;
                  });
        int sum_size = 0;
        for(int j = 1; j <= num_object_label; j++) {
            sum_size += cumsum_write[j][i] - cumsum_delete[j][i];
        }
        int temp_size = 0;
        for (int j = num_object_label - 2; j < num_object_label; j++) {
            not_considered_label[i].insert(temp[j].first);
            // freq_read[temp[j].first][i] = 0;
        }
        // std::cerr << "slice " << i << ": \n";
        // for (int j = 0; j < num_object_label / 2; j++) {
        //     std::cerr << temp[j].first << " ";
        // }
        // std::cerr << std::endl;
        // for (int j = 1; j <= num_object_label; j++) {
        //     std::cerr << j << ":" << freq_read[j][i] << " ";
        // }
        // std::cerr << std::endl;
    }
    // not_considered_label[37].erase(2);
    // not_considered_label[38].erase(3);

    // == 根据freq_read进行聚类
    // 归一化处理
    std::vector<int> first_read_point(num_object_label + 1, 0);
    std::vector<std::vector<double>> normalized_freq_read(num_object_label + 1, std::vector<double>(num_slice + 1, 0.0));
    for (int i = 1; i <= num_object_label; i++) {
        int max_freq = *std::max_element(freq_read[i].begin() + 1, freq_read[i].end());
        for (int j = 1; j <= num_slice; j++) {
            normalized_freq_read[i][j] = static_cast<double>(freq_read[i][j]) / max_freq;
            normalized_freq_read[i][j] = normalized_freq_read[i][j] < 0.1 ? 0 : 1;
            if (normalized_freq_read[i][j] > 0 && first_read_point[i] == 0) {
                first_read_point[i] = j;
            }
        }
    }

    // == 计算磁盘的标签序列
    // 计算距离矩阵
    std::vector<std::vector<double>> distance(num_object_label + 1, std::vector<double>(num_object_label + 1, 0.0));
    for (int i = 1; i <= num_object_label; i++) {
        for (int j = 1; j <= num_object_label; j++) {
            double dist = 0.0;
            for (int l = 1; l <= num_slice; l++) {
                dist += std::pow(normalized_freq_read[i][l] - normalized_freq_read[j][l], 2);
            }
            distance[i][j] = std::sqrt(dist);
        }
    }
    // 贪心找最近邻居
    std::vector<std::vector<int>> label_permutation_list;
    for (int start = 1; start <= num_object_label; start++) {
        std::vector<int> label_permutation(num_object_label);
        std::vector<bool> visited(num_object_label + 1, false);
        int current = 3;
        visited[current] = true;
        label_permutation[0] = current;
        for (int i = 1; i < num_object_label; i++) {
            double min_dist = std::numeric_limits<double>::max();
            int next_node = -1;
            for (int j = 1; j <= num_object_label; j++) {
                if (!visited[j] && distance[current][j] < min_dist) {
                    min_dist = distance[current][j];
                    next_node = j;
                }
            }
            visited[next_node] = true;
            label_permutation[i] = next_node;
            current = next_node;
        }
        label_permutation_list.push_back(label_permutation);
    }

    // == 计算磁盘中label需要的区域大小
    std::vector<std::vector<int>> disk_label_num(num_disk + 1, std::vector<int>(num_object_label + 1, 0));
    std::vector<int> label_object_max_num(num_object_label + 1, 0);
    gen = std::mt19937(SEED);
    std::uniform_real_distribution<> dis(0.6, 1.4);
    long long max_units_sum = 0;
    for (int i = 1; i <= num_object_label; i++) {
        long long sum = 0;
        for (int j = 1; j <= num_slice; j++) {
            sum += cumsum_write[i][j] - cumsum_delete[i][j];
        }
        label_object_max_num[i] = sum / num_slice;
        max_units_sum += label_object_max_num[i];
    }
    std::cerr << max_units_sum << " " << num_disk_unit / REP_NUM * num_disk << std::endl;
    // for (int i = 1; i <= num_object_label; i++) {
    //     label_object_max_num[i] = 1ll * label_object_max_num[i] * num_disk_unit / REP_NUM / max_units_sum;
    // }
    for (int i = 1; i <= num_disk; i++) {
        for (int j = 1; j <= num_object_label; j++) {
            disk_label_num[i][j] = label_object_max_num[j] * dis(gen);
        }
    }
    // // Assign each label to exactly 5 disks with load balancing based on read frequency
    // std::vector<std::vector<bool>> label_assigned(num_disk + 1, std::vector<bool>(num_object_label + 1, false));
    
    // // Calculate total read frequency for each label across all slices
    // std::vector<long long> label_total_freq(num_object_label + 1, 0);
    // for (int j = 1; j <= num_object_label; j++) {
    //     for (int slice = 1; slice <= num_slice; slice++) {
    //         label_total_freq[j] += freq_read[j][slice];
    //     }
    // }
    
    // // Sort labels by their total read frequency (highest to lowest)
    // std::vector<int> labels(num_object_label);
    // std::iota(labels.begin(), labels.end(), 1);
    // std::sort(labels.begin(), labels.end(), [&](int a, int b) {
    //     return label_total_freq[a] > label_total_freq[b];
    // });
    
    // // Track current load per disk (for load balancing)
    // std::vector<long long> disk_loads(num_disk + 1, 0);
    
    // // Assign labels to disks
    // for (int j : labels) {
    //     // Find the 5 least loaded disks
    //     std::vector<std::pair<long long, int>> disk_load_pairs;
    //     for (int i = 1; i <= num_disk; i++) {
    //         disk_load_pairs.push_back({disk_loads[i], i});
    //     }
    //     std::sort(disk_load_pairs.begin(), disk_load_pairs.end());
        
    //     // Calculate objects per disk for this label
    //     int remaining = label_object_max_num[j];
        
    //     // Distribute to the 5 least loaded disks
    //     for (int k = 0; k < 5; k++) {
    //         int disk_id = disk_load_pairs[k].second;
    //         label_assigned[disk_id][j] = true;
            
    //         // Distribute with some randomness, but more evenly
    //         int amount;
    //         if (k == 4) {
    //             // For the last disk, assign all remaining objects
    //             amount = remaining;
    //         } else {
    //             // More even distribution (20-25% per disk)
    //             double distribution_factor = 0.2 + 0.05 * dis(gen) - 0.025;
    //             amount = static_cast<int>(remaining * distribution_factor);
    //             amount = std::max(1, std::min(amount, remaining - (5 - k - 1))); // Ensure we leave some for remaining disks
    //         }
            
    //         // Ensure we don't exceed disk capacity
    //         int disk_used = std::accumulate(disk_label_num[disk_id].begin(), disk_label_num[disk_id].end(), 0);
    //         int disk_capacity = num_disk_unit / REP_NUM;
    //         amount = std::min(amount, disk_capacity - disk_used);
            
    //         disk_label_num[disk_id][j] = amount;
    //         remaining -= amount;
            
    //         // Update the disk load based on the assigned label's read frequency and object count
    //         disk_loads[disk_id] += label_total_freq[j] * amount / label_object_max_num[j];
    //     }
    // }
    // // Verify and output distribution statistics
    // for (int j = 1; j <= num_object_label; j++) {
    //     int total = 0;
    //     int disk_count = 0;
    //     for (int i = 1; i <= num_disk; i++) {
    //         total += disk_label_num[i][j];
    //         if (label_assigned[i][j]) disk_count++;
    //     }
    //     std::cerr << "Label " << j << ": assigned to " << disk_count << " disks, total: " 
    //               << total << "/" << label_object_max_num[j] << std::endl;
    // }
    // for (int i = 1; i <= num_disk; i++) {
    //     for (int j = 1; j <= num_object_label; j++) {
    //         std::cerr << disk_label_num[i][j] << "\t";
    //     }
    //     std::cerr << std::endl;
    // }

    // == 初始化磁盘
    disks.resize(num_disk + 1);
    std::vector<double> label_num_norm(num_object_label + 1, 0.0);
    for (int i = 1; i <= num_disk; i++) {
        int total_label_object_max_num = accumulate(disk_label_num[i].begin() + 1, disk_label_num[i].end(), 0);
        for (int j = 0; j < num_object_label; j++) {
            auto label = label_permutation_list[i][j];
            label_num_norm[label] = 1.0 * disk_label_num[i][label] / total_label_object_max_num;
        }
        disks[i].init(i, num_disk_unit, label_num_norm, label_permutation_list[i]);
    }

    // 初始化统计变量
    stat_read_speed.resize(num_disk + 1, std::vector<std::vector<int>>(HEAD_NUM + 1));
    stat_obj_num.resize(num_disk + 1, std::vector<int>(HEAD_NUM + 1, 0));

    // == 方便后续交互，增加额外时间
    num_timestamp += EXTRA_TIME;

    // == 输出OK，结束初始化
    printf("OK\n");
    fflush(stdout);
}

void Controller::interact() {
    for (current_timestamp = 1; current_timestamp <= num_timestamp; current_timestamp++) {
        // std::cerr << "TIMESTAMP " << current_timestamp << std::endl;
        timestamp_action();
        delete_action();
        // std::cerr << "DELETE DONE" << std::endl;
        write_action();
        // std::cerr << "WRITE DONE" << std::endl;
        read_action();
        // std::cerr << "READ DONE" << std::endl;
        if (current_timestamp % FRE_PER_SLICING == 0) {
            gc_action();
            // std::cerr << "GC DONE" << std::endl;
        }
    }
    // print_stat_info();
}

void Controller::print_stat_info() {
    // 统计读速度
    std::ofstream speed_file("speed.txt", std::ios::app);
    for (int i = 1; i <= num_disk; i++) {
        for (int j = 1; j <= HEAD_NUM; j++) {
            std::cerr << "Disk " << i << " Head " << j << ": ";
            auto& times = stat_read_speed[i][j];
            if (times.empty()) {
                std::cerr << "No data" << std::endl;
            } else {
                // Calculate time differences
                std::vector<int> diffs;
                for (size_t k = 1; k < times.size(); k++) {
                    diffs.push_back(times[k] - times[k - 1]);
                }
                // Write differences to a file for analysis
                if (speed_file.is_open()) {
                    speed_file << "Disk " << i << " Head " << j << ":\n";
                    for (const auto& diff : diffs) {
                        speed_file << diff << "\n";
                    }
                    speed_file << "\n";
                }

                // Calculate statistics
                double mean = 0.0;
                int min_val = INT_MAX;
                int max_val = INT_MIN;

                if (!diffs.empty()) {
                    mean = std::accumulate(diffs.begin(), diffs.end(), 0.0) / diffs.size();
                    min_val = *std::min_element(diffs.begin(), diffs.end());
                    max_val = *std::max_element(diffs.begin(), diffs.end());
                }

                // Calculate variance
                double variance = 0.0;
                for (auto diff : diffs) {
                    variance += (diff - mean) * (diff - mean);
                }
                if (!diffs.empty()) {
                    variance /= diffs.size();
                }

                std::cerr << "Count: " << times.size()
                          << ", Mean: " << mean
                          << ", Variance: " << variance
                          << ", Std Dev: " << std::sqrt(variance)
                          << ", Min: " << min_val
                          << ", Max: " << max_val
                          << ", Obj Count:" << stat_obj_num[i][j]
                          << std::endl;
            }
        }
    }
    speed_file.close();
}
