#!/bin/bash

# 获取当前时间
current_time=$(date +"%Y%m%d_%H%M%S")

# 压缩文件名
archive_name="preliminary_${current_time}.zip"

# 压缩
cd src
zip -r $archive_name ./ -i '*.cpp' '*.hpp' 'CMakeLists.txt'

# 将压缩文件移动到submit目录
mv $archive_name ../submit/