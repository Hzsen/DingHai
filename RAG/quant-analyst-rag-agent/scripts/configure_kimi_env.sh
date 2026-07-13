#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="${0:A:h}"
PROJECT_ROOT="${SCRIPT_DIR:h}"
ENV_FILE="${PROJECT_ROOT}/.env"

printf "请输入 Kimi API Key（输入不会回显）: " >&2
IFS= read -r -s api_key
printf "\n" >&2

if [[ -z "${api_key}" ]]; then
  printf "Key 不能为空，未修改 .env。\n" >&2
  exit 1
fi

if [[ "${api_key}" != sk-* ]]; then
  printf "Key 格式异常：Kimi API Key 通常以 sk- 开头，未修改 .env。\n" >&2
  exit 1
fi

umask 077
temp_file="$(mktemp "${ENV_FILE}.tmp.XXXXXX")"
trap 'rm -f "${temp_file}"' EXIT

printf '%s\n' \
  '# Local secrets. This file is ignored by Git.' \
  "MOONSHOT_API_KEY=${api_key}" \
  'MOONSHOT_BASE_URL=https://api.moonshot.cn/v1' \
  'MOONSHOT_MODEL=kimi-k2.6' > "${temp_file}"

mv "${temp_file}" "${ENV_FILE}"
chmod 600 "${ENV_FILE}"
unset api_key
trap - EXIT

printf "Kimi API 环境已写入 %s（权限 600，Key 未回显）。\n" "${ENV_FILE}"
