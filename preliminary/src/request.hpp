#pragma once
#include "utils.hpp"

class Request {
   public:
    int id;               // 请求的id
    int object_id;        // 请求的对象id
    int start_timestamp;  // 请求开始的时间片
    bool is_done;         // 请求是否完成
};