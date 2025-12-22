#!/bin/bash
set -e  # 脚本执行出错时立即退出

##############################################################################
# 配置参数（可根据需求修改，均为用户可访问路径）
##############################################################################
DOLPHINDB_VERSION="3.00.3"  # 需部署的 DolphinDB 版本
# 安装目录：默认用户主目录下的 dolphindb 文件夹（无需sudo）
INSTALL_DIR="${HOME}/dolphindb"
MAX_MEM_SIZE="2"             # 最大可用内存（GB，普通用户建议0.5-1，避免资源超限）
PORT="8848"                  # 服务端口（默认8848，冲突时可修改为1024+端口）

##############################################################################
# 1. 检查系统架构（自动识别 x86_64/ARM64）
##############################################################################
echo "=== 1. 检测系统架构 ==="
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)
        PACKAGE_TYPE="Linux64"
        echo "已识别架构：x86_64，将下载 Linux64 版本"
        ;;
    aarch64)
        PACKAGE_TYPE="ARM64"
        echo "已识别架构：ARM64，将下载 ARM64 版本"
        ;;
    *)
        echo "错误：不支持的架构 '$ARCH'，仅支持 x86_64（aarch64）"
        exit 1
        ;;
esac

##############################################################################
# 2. 检查依赖工具（wget/unzip，仅检查是否存在，不涉及系统安装）
##############################################################################
echo -e "\n=== 2. 检查依赖工具 ==="
REQUIRED_TOOLS=("wget" "unzip")
for TOOL in "${REQUIRED_TOOLS[@]}"; do
    if ! command -v "$TOOL" &> /dev/null; then
        echo "错误：未找到工具 '$TOOL'，请联系管理员或通过用户级安装（如 Miniconda 安装）"
        exit 1
    fi
done
echo "所有依赖工具已就绪"

##############################################################################
# 3. 准备安装目录（用户主目录内，无需sudo）
##############################################################################
echo -e "\n=== 3. 准备安装目录 ==="
if [ -d "$INSTALL_DIR" ]; then
    echo "警告：安装目录 '$INSTALL_DIR' 已存在，将删除旧文件（仅用户自有文件）"
    rm -rf "$INSTALL_DIR"
fi
# mkdir -p 无需sudo，用户对自己的主目录有完全权限
mkdir -p "$INSTALL_DIR"
echo "安装目录 '$INSTALL_DIR' 已创建"

##############################################################################
# 4. 下载 DolphinDB 安装包（临时文件存用户缓存目录）
##############################################################################
echo -e "\n=== 4. 下载 DolphinDB $DOLPHINDB_VERSION ($PACKAGE_TYPE) ==="
DOWNLOAD_URL="https://www.dolphindb.cn/downloads/DolphinDB_${PACKAGE_TYPE}_V${DOLPHINDB_VERSION}.zip"
# 临时压缩包存用户缓存目录（避免/tmp权限问题）
TEMP_ZIP="${HOME}/.cache/dolphindb_${PACKAGE_TYPE}_V${DOLPHINDB_VERSION}.zip"
mkdir -p "${HOME}/.cache"  # 确保缓存目录存在

# 开始下载并显示进度
if wget -O "$TEMP_ZIP" "$DOWNLOAD_URL" --progress=bar:force; then
    echo "下载成功：$DOWNLOAD_URL"
else
    echo "错误：下载失败，请检查版本号、网络连接或目标地址可访问性"
    rm -f "$TEMP_ZIP"  # 清理失败的压缩包
    exit 1
fi

##############################################################################
# 5. 解压安装包（用户目录内操作，无需权限）
##############################################################################
echo -e "\n=== 5. 解压安装包 ==="
# -q 静默解压，-d 指定目标目录（用户可写）
unzip -q "$TEMP_ZIP" -d "$INSTALL_DIR"
rm -f "$TEMP_ZIP"  # 解压后删除临时压缩包，节省空间

# 检查解压结果（确保 server 目录存在）
SERVER_DIR="${INSTALL_DIR}/server"
if [ ! -d "$SERVER_DIR" ]; then
    echo "错误：解压失败，未找到 server 目录，请检查压缩包完整性"
    exit 1
fi
echo "解压完成，server 目录：$SERVER_DIR"

##############################################################################
# 6. 配置 DolphinDB（修改内存、端口等参数，适配普通用户）
##############################################################################
echo -e "\n=== 6. 配置 DolphinDB 参数 ==="
CFG_FILE="${SERVER_DIR}/dolphindb.cfg"

# 备份默认配置（用户目录内操作，安全可逆）
cp "$CFG_FILE" "${CFG_FILE}.bak"

# 修改核心配置参数（适配普通用户资源限制）
# 1. 端口配置（避免使用1024以下端口，普通用户无权限）
sed -i "s/^localSite=.*/localSite=localhost:${PORT}:local${PORT}/" "$CFG_FILE"
# 2. 最大内存限制（降低默认值，避免普通用户内存超限）
sed -i "s/^maxMemSize=.*/maxMemSize=${MAX_MEM_SIZE}/" "$CFG_FILE"

echo "配置完成，核心参数（适配普通用户）："
echo "  - 服务端口：$PORT（1024+，普通用户可使用）"
echo "  - 最大内存：${MAX_MEM_SIZE}GB（避免资源超限）"

##############################################################################
# 7. 授权可执行文件（仅用户自有文件，无需sudo）
##############################################################################
echo -e "\n=== 7. 授权可执行权限 ==="
# 仅对用户自己的安装目录内文件授权，无需系统权限
chmod +x "${SERVER_DIR}/dolphindb"  # 主程序授权
chmod +x "${SERVER_DIR}/startSingle.sh"  # 后台启动脚本授权
echo "可执行文件权限已配置（仅用户自有文件）"

##############################################################################
# 8. 启动 DolphinDB（后台运行，普通用户进程）
##############################################################################
echo -e "\n=== 8. 启动 DolphinDB 服务 ==="
cd "$SERVER_DIR" || exit 1  # 切换到 server 目录（用户可访问）
# 后台启动（不依赖系统服务，仅普通用户进程）
sh startSingle.sh

# 等待 3 秒，确保服务有足够时间启动（普通用户进程启动可能稍慢）
sleep 3

##############################################################################
# 9. 校验服务状态（普通用户可执行的进程/端口检查）
##############################################################################
echo -e "\n=== 9. 校验服务状态 ==="
# 检查进程是否存在（仅匹配当前用户的 dolphindb 进程）
PROCESS_COUNT=$(ps -u "$USER" | grep -v grep | grep "dolphindb" | wc -l)
if [ "$PROCESS_COUNT" -ge 1 ]; then
    echo "✅ DolphinDB 进程已启动（当前用户进程数：$PROCESS_COUNT）"
else
    echo "❌ DolphinDB 进程未检测到，启动失败"
    if [ -f "${SERVER_DIR}/dolphindb.log" ]; then
        echo "日志片段（最后 15 行，帮助排查）："
        tail -15 "${SERVER_DIR}/dolphindb.log"
    fi
    exit 1
fi

# 修正后的端口检查命令
if ss -tuln | grep -q ":${PORT}\b"; then
    echo "✅ 端口 ${PORT} 已监听"
else
    echo "⚠️  端口 ${PORT} 未监听，可能服务未完全启动（建议等待 5 秒后执行：ss -tuln | grep ${PORT}）"
fi

##############################################################################
# 10. 输出访问信息（用户可操作的命令）
##############################################################################
echo -e "\n=== 部署完成（无需 sudo 权限） ==="
# 获取用户可访问的 IP（优先内网IP，避免公网访问问题）
IP_ADDR=$(hostname -I | awk '{print $1}' || echo "127.0.0.1")
echo "📌 服务信息（当前用户专属）："
echo "   - 安装目录：$INSTALL_DIR（仅当前用户可访问）"
echo "   - Web 管理界面：http://${IP_ADDR}:${PORT}（本地/内网访问）"
echo "   - 停止服务：cd ${SERVER_DIR} && sh clusterDemo/stopAllNode.sh（仅当前用户进程）"
echo "   - 查看日志：tail -f ${SERVER_DIR}/dolphindb.log（用户可读写）"
echo -e "\n提示：若端口冲突，修改脚本中 'PORT' 参数为 1024-65535 之间未占用的端口"