#!/bin/bash

# 定义 JSON 文件所在目录
JSON_DIR="/home/scripts/datax_json"

# 定义 datax.py 的路径（请根据你实际路径修改！）
DATAX_PY="/opt/installs/datax/bin/datax.py"


echo "========================================"
echo "开始执行目录下所有 DataX 任务：$JSON_DIR"
echo "========================================"

# 循环遍历所有 .json 文件
for json_file in "$JSON_DIR"/*.json; do
    # 如果目录里没有 json 文件，跳过
    [ -e "$json_file" ] || continue

    echo ""
    echo "====> 正在执行：$json_file"

    # 执行 DataX 命令
    python "$DATAX_PY" "$json_file"
done

echo "========================================"
echo "所有任务执行完成！"
echo "========================================"