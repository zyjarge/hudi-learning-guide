#!/bin/bash
# 配置Trino catalog

echo "等待Trino启动..."
MAX_WAIT=120
WAITED=0
until trino --execute "SELECT 1" 2>/dev/null; do
  if [ $WAITED -ge $MAX_WAIT ]; then
    echo "超时：Trino未能在 $MAX_WAIT 秒内启动"
    exit 1
  fi
  echo "Trino未就绪，等待中... ($WAITED/$MAX_WAIT)"
  sleep 5
  WAITED=$((WAITED + 5))
done

echo "Trino已启动，正在配置catalog..."

# 测试连接
echo "测试Trino连接..."
trino --execute "SHOW CATALOGS"

echo "Trino catalog配置完成！"
echo ""
echo "可用的catalog："
trino --execute "SHOW CATALOGS"