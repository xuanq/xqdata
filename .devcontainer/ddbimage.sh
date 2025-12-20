#!/bin/bash

# 定义镜像基础名称（根据你的实际情况修改）
BASE_IMAGE="dolphindb/dolphindb"
TARGET_TAG="auto"

# 获取架构信息
ARCH=$(uname -m)
echo "检测到宿主机架构: $ARCH"

if [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then
    SOURCE_IMAGE="${BASE_IMAGE}-arm64:v3.00.3"
elif [[ "$ARCH" == "x86_64" ]]; then
    SOURCE_IMAGE="${BASE_IMAGE}:v3.00.4"
else
    echo "未知的架构: $ARCH"
    exit 1
fi

echo "正在准备镜像: $SOURCE_IMAGE -> ${BASE_IMAGE}:${TARGET_TAG}"

# 检查本地是否已经存在该 Tag（可选：如果不想每次启动都 pull）
if [[ "$(docker images -q ${BASE_IMAGE}:${TARGET_TAG} 2> /dev/null)" == "" ]]; then
    echo "本地未找到缓存镜像，正在拉取..."
    docker pull $SOURCE_IMAGE
    docker tag $SOURCE_IMAGE "${BASE_IMAGE}:${TARGET_TAG}"
else
    echo "本地已存在 ${TARGET_TAG}，跳过拉取。"
fi