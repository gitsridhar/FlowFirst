#!/usr/bin/env bash
# ==============================================================================
# FlowFirst - EXTRA_NODES helper (source this file; do not execute it)
#
# EXTRA_NODES in .env lists additional FULL core nodes beyond mgmtnode1..mgmtnode3:
#     EXTRA_NODES=extramgmtnode1:10.0.0.7,extramgmtnode2:10.0.0.8
# Blank (default) = the classic three-node cluster; nothing below changes anything.
# remotenode1 is NOT an extra node: it is the remote worker (Process 5 only).
#
# After `extra_nodes_load` these are set:
#   EXTRA_NAMES[]   node names, in the order listed
#   EXTRA_IPS[]     matching IPv4 addresses
#   EXTRA_COUNT     number of extra nodes
# Derived, stable IDs (they depend on the number in the name, never on list order, and are
# never reused): ZooKeeper myid and Swift zone for extramgmtnode<N> are 3+N  (N=1 -> 4).
#
# The parser is strict (allow-list regexes) and exits non-zero on any bad entry, so a typo
# can never inject text into a generated config file. It never prints more than the bad entry's
# position.
# ==============================================================================

EXTRA_NAMES=()
EXTRA_IPS=()
EXTRA_COUNT=0

_EXTRA_NAME_RE='^extramgmtnode[0-9]+$'
_EXTRA_IPV4_RE='^((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])$'

extra_nodes_load() {
    local raw="${1-${EXTRA_NODES:-}}"
    local item name ip pos=0 i
    EXTRA_NAMES=()
    EXTRA_IPS=()
    EXTRA_COUNT=0
    raw="${raw//[[:space:]]/}"
    [ -z "${raw}" ] && return 0
    local items=()
    IFS=',' read -r -a items <<< "${raw}"
    for item in "${items[@]}"; do
        pos=$((pos + 1))
        name="${item%%:*}"
        ip="${item#*:}"
        if [[ "${item}" != *:* ]] || ! [[ "${name}" =~ ${_EXTRA_NAME_RE} ]] || ! [[ "${ip}" =~ ${_EXTRA_IPV4_RE} ]]; then
            echo "ERROR: EXTRA_NODES entry #${pos} is invalid (expected extramgmtnode<N>:<IPv4>)." >&2
            return 1
        fi
        for i in "${!EXTRA_NAMES[@]}"; do
            if [ "${EXTRA_NAMES[$i]}" = "${name}" ] || [ "${EXTRA_IPS[$i]}" = "${ip}" ]; then
                echo "ERROR: EXTRA_NODES entry #${pos} duplicates an earlier name or IP." >&2
                return 1
            fi
        done
        EXTRA_NAMES+=("${name}")
        EXTRA_IPS+=("${ip}")
    done
    EXTRA_COUNT=${#EXTRA_NAMES[@]}
}

# extra_node_num extramgmtnode3 -> 3
extra_node_num() { echo "${1#extramgmtnode}"; }

# extra_node_id extramgmtnode3 -> 6   (ZooKeeper myid and Swift zone)
extra_node_id() { echo $((3 + $(extra_node_num "$1"))); }

# extra_name_for_ip 10.0.0.7 -> extramgmtnode1 (empty + status 1 when the IP is not an extra node)
extra_name_for_ip() {
    local i
    for i in "${!EXTRA_IPS[@]}"; do
        if [ "${EXTRA_IPS[$i]}" = "$1" ]; then
            echo "${EXTRA_NAMES[$i]}"
            return 0
        fi
    done
    return 1
}

# extra_ips_csv -> "10.0.0.7,10.0.0.8"  (empty when there are no extra nodes)
extra_ips_csv() {
    local out="" ip
    for ip in "${EXTRA_IPS[@]+"${EXTRA_IPS[@]}"}"; do
        out="${out:+${out},}${ip}"
    done
    echo "${out}"
}
