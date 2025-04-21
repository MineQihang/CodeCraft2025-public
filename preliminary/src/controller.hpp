#pragma once
#include "utils.hpp"
#include "disk.hpp"
#include "request.hpp"
#include "object.hpp"

class Controller {
   private:
    // 输入
    int num_timestamp;     // 时间片数量, T in [1+105, 86400+105]
    int num_object_label;  // 对象标签数量, M in [1, 16]
    int num_disk;          // 磁盘数量, N in [3, 10]
    int num_disk_unit;     // 磁盘单元数量, V in [1, 16384]
    int max_num_token;     // 最大令牌数量, G in [64, 1000]
    std::vector<std::vector<int>> freq_delete;  // 删除操作频率
    std::vector<std::vector<int>> freq_write;   // 写操作频率
    std::vector<std::vector<int>> freq_read;    // 读操作频率
    int num_slice;                              // 频率数量

    // 主要数据
    std::vector<Disk> disks;
    Request requests[MAX_REQUEST_NUM];
    Object objects[MAX_OBJECT_NUM];

    // 中间变量
    int current_timestamp;          // 当前时间戳
    int current_slice;              // 当前时间片
    std::vector<int> active_object; // 当前被read的对象
    std::set<int> ignored_obj;      // 被忽略的对象
    std::mt19937 gen;               // 随机数生成器

    // 操作函数
    void timestamp_action();
    void do_object_delete(Object& object);
    void delete_action();
    void do_object_write(Object& object, int prv_disk);
    void write_action();
    void do_request_add(Request& request, Object& object);
    void read_action_for_disk(Disk& disk, std::stringstream& action_ss);
    void read_action();

   public:
    // 外部函数
    void init();
    void interact();
};

void Controller::timestamp_action() {
    int timestamp;
    scanf("%*s%d", &timestamp);

    current_slice = GET_FREQ(timestamp);
    assert(timestamp == current_timestamp && "Timestamp error");

    printf("TIMESTAMP %d\n", timestamp);
    fflush(stdout);
}

void Controller::do_object_delete(Object& object) {
    for (int i = 1; i <= REP_NUM; i++) {
        Disk& disk = disks[object.replica[i]];
        disk.delete_units(object.units[i]);
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
    int min_value = INT_MAX;
    int min_value_disk;
    std::vector<int> min_value_units;
    for (int i = 1; i <= num_disk; i++) {
        if (prv_disk != -1 && i != prv_disk) {
            continue;
        }
        auto& disk = disks[i];
        std::vector<int> units;
        int value = disk.try_add_object(object, units);
        if (value < min_value) {
            min_value = value;
            min_value_disk = i;
            min_value_units = units;
        }
    }

    // 写入磁盘
    for (int i = 1; i <= REP_NUM; i++) {
        auto& disk = disks[(min_value_disk + i - 2) % num_disk + 1];
        disk.add_object(object, min_value_units, i);
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
    request.is_done = false;
    object.request_queue.push(request.id);
}

void Controller::read_action_for_disk(Disk& disk, std::stringstream& action_ss) {
    auto& disk_unit = disk.units;
    auto& disk_point = disk.point;
    auto& disk_last_op = disk.last_op;
    auto& disk_last_token = disk.last_token;

    // 判断是不是该跳转了
    int step = disk.get_next_read_step(objects);
    if (step >= max_num_token && disk_point + step <= num_disk_unit / REP_NUM) {
        action_ss << "j " << disk_point + step << "\n";
        disk_point = disk_point + step;
        disk_last_op = 'j';
        disk_last_token = FIRST_READ_TOKEN;
        return;
    } else if (step == -1) {
        int best_point = disk.get_best_jump_point(objects);
        action_ss << "j " << best_point << "\n";
        disk_point = best_point;
        disk_last_op = 'j';
        disk_last_token = FIRST_READ_TOKEN;
        return ;
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
                return calc_rp(
                    current_point,
                    current_token, 
                    'p', 
                    FIRST_READ_TOKEN, 
                    true,
                    action
                );
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
                    temp_action
                );
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
                    read_action
                );
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
                action
            );
        }
    };
    std::string best_action;
    bool last_empty_ = disk_unit[BACKWARD_STEP(disk_point)] == 0 || objects[disk_unit[BACKWARD_STEP(disk_point)]].request_queue.size() == 0;
    calc_rp(disk_point, max_num_token * 1.25, disk_last_op, disk_last_token, last_empty_, best_action);

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
            if(valid_token < 1) {
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
    if(valid_token < 0) {
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
    std::vector<std::string> disk_action(num_disk + 1);
    for (int j = 0; j < num_disk; j++) {
        auto i = disk_permutation[j];
        std::stringstream action_ss;
        read_action_for_disk(disks[i], action_ss);
        disk_action[i] = action_ss.str();
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
                    assert(false && "Request is done");
                }
                object.request_queue.pop();
            }
        }
        active_object.clear();
    }

    for (int i = 1; i <= num_disk; i++) {
        printf("%s", disk_action[i].c_str());
    }

    printf("%d\n", success_requests.size());
    for (auto& request_id : success_requests) {
        printf("%d\n", request_id);
    }
    
    fflush(stdout);
}

void Controller::init() {
    // == 输入基本信息
    scanf("%d%d%d%d%d", &num_timestamp, &num_object_label, &num_disk,
          &num_disk_unit, &max_num_token);
    
    // == 输入对象label的特征
    freq_delete.resize(num_object_label + 1);
    freq_write.resize(num_object_label + 1);
    freq_read.resize(num_object_label + 1);
    num_slice = GET_FREQ(num_timestamp);
    std::vector<std::vector<int>> cumsum_delete(num_object_label + 1);
    std::vector<std::vector<int>> cumsum_write(num_object_label + 1);
    std::vector<std::vector<int>> cumsum_read(num_object_label + 1);
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
    for (int i = 1; i <= num_object_label; i++) {
        freq_read[i].resize(num_slice + 1);
        cumsum_read[i].resize(num_slice + 1, 0);
        for (int j = 1; j <= num_slice; j++) {
            scanf("%d", &freq_read[i][j]);
            cumsum_read[i][j] = cumsum_read[i][j - 1] + freq_read[i][j];
        }
    }

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
    std::vector<int> label_permutation(num_object_label);
    std::vector<bool> visited(num_object_label + 1, false);
    int current = 1;
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
    // for (int i = 0; i < num_object_label; i++) {
    //     std::cerr << label_permutation[i] << " ";
    // }
    // std::cerr << std::endl;

    // == 计算磁盘中label需要的区域大小
    std::vector<std::vector<int>> disk_label_num(num_disk + 1, std::vector<int>(num_object_label + 1, 0));
    std::vector<int> label_object_max_num(num_object_label + 1, 0);
    gen = std::mt19937(SEED);
    std::uniform_real_distribution<> dis(0.8, 1.2);
    for (int i = 1; i <= num_object_label; i++) {
        for (int j = 1; j <= num_slice; j++) {
            label_object_max_num[i] = std::max(label_object_max_num[i], cumsum_write[i][j] - cumsum_delete[i][j]);
        }
    }
    for (int i = 1; i <= num_disk; i++) {
        for (int j = 1; j <= num_object_label; j++) {
            disk_label_num[i][j] = label_object_max_num[j] * dis(gen);
        }
    }
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
            auto label = label_permutation[j];
            label_num_norm[label] = 1.0 * disk_label_num[i][label] / total_label_object_max_num;
        }
        disks[i].init(i, num_disk_unit, label_num_norm, label_permutation);
    }

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
    }
}
