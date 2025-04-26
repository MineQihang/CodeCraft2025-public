#pragma once
#include "disk_round1.hpp"
#include "disk_round2.hpp"
#include "object.hpp"
#include "request.hpp"
#include "utils.hpp"

class Controller {
   private:
    // 输入
    int num_timestamp;     // 时间片数量, T in [1+105, 86400+105]
    int num_object_label;  // 对象标签数量, M in [1, 16]
    int num_disk;          // 磁盘数量, N in [3, 10]
    int num_disk_unit;     // 磁盘单元数量, V in [1, 16384]
    int max_num_token;     // 最大令牌数量, G in [64, 500]
    int max_swap_num_round1;      // 每个硬盘最多的交换存储单元的操作次数, K in [0, 100]
    int max_swap_num_round2;      // 每个硬盘最多的交换存储单元的操作次数, K in [0, 100]
    int max_swap_num;      // 每个硬盘最多的交换存储单元的操作次数, K in [0, 100]

    // 主要数据
    std::vector<DiskRound1> disks1;     // 第一轮的磁盘
    std::vector<DiskRound2> disks2;     // 第二轮的磁盘, [round2会修改]
    Request requests[MAX_REQUEST_NUM];  // 请求, [round2会修改]
    Object objects[MAX_OBJECT_NUM];     // 对象, [round2会修改]

    // 第一轮记录（第二轮需要用到的）
    std::vector<int> delete_datas[MAX_TIME_SLICING];
    std::vector<std::tuple<int, int, int>> write_datas[MAX_TIME_SLICING];
    std::vector<std::tuple<int, int>> read_datas[MAX_TIME_SLICING];
    std::map<int, int> object_label;

    // 中间变量
    int num_slice;                                 // 频率数量
    int current_round;                             // 当前回合
    int current_timestamp;                         // 当前时间戳, [round2会修改]
    int current_slice;                             // 当前时间片, [round2会修改]
    std::vector<int> active_object;                // 当前被read的对象
    std::mt19937 gen;                              // 随机数生成器
    std::uniform_int_distribution<int> label_dis;  // 随机数分布
    std::queue<int> delete_queue;                  // 删除队列, [round2会修改]
    std::vector<int> busy_requests;                // 繁忙的请求
    std::set<int> need_calibrate_obj;              // 需要校准的obj
    std::map<int, std::set<int>> need_gc_obj;      // 校准后准备gc的obj
    std::set<int> not_considered_label;            // 不考虑的label
    double once_score;
    std::stringstream ss_round2;
    std::set<int> abort_requests;

    // 统计变量
    std::vector<std::vector<std::vector<int>>> stat_read_speed;  // 读速度统计, [round2会修改]
    std::vector<std::vector<int>> stat_obj_num;                  // 对象数量统计
    std::vector<std::vector<double>> stat_label_read_num;        // 每个label的请求数量统计
    std::vector<int> stat_label_num;                             // 当前时间片一个label的obj数量
    int stat_unknown_num;
    std::vector<std::vector<double>> freq_read;
    std::vector<std::vector<double>> freq_read_slice;

    std::vector<std::vector<int>> stat_label_read_num_slice;  // 每个label的请求数量统计,粒度为STAT_FREQ_ROUND1
    // std::vector<int> stat_label_num_slice;                             // 当前粒度一个label的obj数量

    std::vector<std::vector<double>> freq_read_round1;
    std::vector<std::vector<double>> freq_delete_round1;
    std::vector<std::vector<double>> freq_write_round1;

    std::vector<std::vector<int>> label_left; // 每个label的剩余数量

    std::vector<std::vector<int>> label_size;  // 磁盘操作记录

    // DEBUG
    double all_score;   // 最大分数
    double read_score;  // 读取到的分数
    double busy_score;  // busy的分数

    // 操作函数
    // Round 1
    void do_object_delete_round1(Object& object);
    void do_object_write_round1(Object& object, int prv_disk);
    void do_request_add_round1(Request& request, Object& object);
    void do_disk_read_round1(DiskRound1& disk, int head_id, std::stringstream& action_ss);
    void delete_action_round1();
    void write_action_round1();
    void read_action_round1();
    void gc_action_round1();
    // Round 2
    void do_object_delete_round2(Object& object);
    void do_object_write_round2(Object& object, int prv_disk);
    void do_request_add_round2(Request& request, Object& object);
    void do_disk_read_round2(DiskRound2& disk, int head_id, std::stringstream& action_ss);
    void delete_action_round2();
    void write_action_round2();
    void read_action_round2();
    void gc_action_round2();
    void update_round2();
    void simulate_round2();
    void simulate_round2_once();
    // Common
    void timestamp_action();
    void start_round2();
    void incre_action();
    void calibrate(bool check_read_size);
    void test_accuracy();

    void stat();

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

void Controller::init() {
    // == 输入基本信息
    scanf("%d%d%d%d%d%d%d", &num_timestamp, &num_object_label, &num_disk, &num_disk_unit, &max_num_token, &max_swap_num_round1, &max_swap_num_round2);

    // == 初始化基础数据
    current_round = 1;
    num_slice = GET_FREQ(num_timestamp);
    gen = std::mt19937(SEED);
    label_dis = std::uniform_int_distribution<int>(1, num_object_label);
    stat_obj_num.resize(num_disk + 1, std::vector<int>(HEAD_NUM + 1, 0));
    stat_read_speed.resize(num_disk + 1, std::vector<std::vector<int>>(HEAD_NUM + 1));
    for (int i = 1; i <= num_disk; i++) {
        for (int j = 1; j <= HEAD_NUM; j++) {
            stat_obj_num[i][j] = 0;
        }
    }
    stat_label_read_num.resize(MAX_TIME_SLICING, std::vector<double>(num_object_label + 1, 0));
    stat_label_num.resize(num_object_label + 1, 0);

    stat_label_read_num_slice.resize(MAX_TIME_SLICING/STAT_FREQ_ROUND1 + 2 , std::vector<int>(num_object_label + 1, 0));
    freq_read_round1.resize(MAX_TIME_SLICING/STAT_FREQ_ROUND1 + 2 , std::vector<double>(num_object_label + 1, 0));
    freq_write_round1.resize(MAX_TIME_SLICING/STAT_FREQ_ROUND1 + 2 , std::vector<double>(num_object_label + 1, 0));
    freq_delete_round1.resize(MAX_TIME_SLICING/STAT_FREQ_ROUND1 + 2 , std::vector<double>(num_object_label + 1, 0));
    label_left.resize(MAX_TIME_SLICING/STAT_FREQ_ROUND1 + 2 , std::vector<int>(num_object_label + 1, 0));
    
    label_size.resize(num_object_label + 1, std::vector<int>(MAX_OBJECT_SIZE + 1, 0));

    // == 初始化磁盘
    disks1.resize(num_disk + 1);
    for (int i = 1; i <= num_disk; i++) {
        disks1[i].init(i, num_disk_unit, num_object_label);
    }

    // == 方便后续交互，增加额外时间
    num_timestamp += EXTRA_TIME;

    // == 输出OK，结束初始化
    printf("OK\n");
    fflush(stdout);
}

void Controller::interact() {
    max_swap_num = max_swap_num_round1;
    // Round 1
    for (current_timestamp = 1; current_timestamp <= num_timestamp; current_timestamp++) {
        timestamp_action();
        delete_action_round1();
        write_action_round1();
        read_action_round1();
        if (current_timestamp % FRE_PER_SLICING == 0) {
            calibrate(true);
            gc_action_round1();
        }
    }
    // stat();

    max_swap_num = max_swap_num_round2;
    // Round 2
    start_round2();
    for (current_timestamp = 1; current_timestamp <= num_timestamp; current_timestamp++) {
        timestamp_action();
        delete_action_round2();
        write_action_round2();
        read_action_round2();
        if (current_timestamp % FRE_PER_SLICING == 0) {
            gc_action_round2();
        }
        // std::cerr << ss_round2.str() << std::endl;
        printf("%s", ss_round2.str().c_str());
        ss_round2.str("");
        ss_round2.clear();
        fflush(stdout);
        if (current_timestamp % FRE_PER_SLICING == 0) {
            simulate_round2();
            update_round2();
        }
#ifdef DEBUG_ROUND_2
        if (current_timestamp == num_timestamp - 1) {
            std::cerr.precision(3);
            std::cerr << "all_score: " << std::fixed << all_score << std::endl;
            std::cerr << "read_score: " << std::fixed << read_score << std::endl;
            std::cerr << "busy_score: " << std::fixed << busy_score << std::endl;
            std::cerr << "read_score / all_score: " << std::fixed << 100 * read_score / all_score << "%" << std::endl;
            std::cerr << "busy_score / all_score: " << std::fixed << 100 * busy_score / all_score << "%" << std::endl;
            std::cerr << "(read_score - busy_score) / all_score: " << std::fixed << 100 * (read_score - busy_score) / all_score << "%" << std::endl;
            std::ofstream file("data/speed.txt");
            if (!file.is_open()) {
                std::cerr << "Failed to open ground truth file" << std::endl;
                return;
            }
            for (int i = 1; i <= num_disk; i++) {
                for (int j = 1; j <= HEAD_NUM; j++) {
                    file << stat_read_speed[i][j].size() << " ";
                    for (auto& speed : stat_read_speed[i][j]) {
                        file << speed << " ";
                    }
                    file << std::endl;
                }
            }
            file.close();
        }
#endif
    }
}