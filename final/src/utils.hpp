#pragma once
#include <bits/stdc++.h>

// 题目参数
#define MAX_DISK_NUM (10 + 1)
#define MAX_DISK_SIZE (16384 + 1)
#define MAX_REQUEST_NUM (30000000 + 1)
#define MAX_OBJECT_NUM (100000 + 1)
#define REP_NUM (3)
#define FRE_PER_SLICING (1800)
#define EXTRA_TIME (105)
#define MAX_TIME_SLICING (86400 + EXTRA_TIME + 1)
#define FIRST_READ_TOKEN (64)
#define MIN_READ_TOKEN (16)
#define MIN_OBJECT_SIZE (1)
#define MAX_OBJECT_SIZE (5)
#define HEAD_NUM (2)

// 工具
#define GET_FREQ(x) ((x - 1) / FRE_PER_SLICING + 1)
#define FORWARD_STEP(x) (x % num_disk_unit + 1)
#define BACKWARD_STEP(x) (x == 1 ? num_disk_unit : x - 1)
#define NEXT_TOKEN(x) std::max(MIN_READ_TOKEN, int(ceil(x * 0.8)))
#define GET_DISK(x) (current_round == 1 ? static_cast<Disk&>(disks1[x]) : static_cast<Disk&>(disks2[x]))

// 参数
#define MAX_K (3)
#define SEED (42)
#define TOKEN_CONSIDERED_RATE (1)
#define FREQ (100)