#!/usr/bin/env bash
# simulate_network_glitch.sh — inject and clear network faults on a cluster node
# Usage: sudo ./scripts/simulate_network_glitch.sh <command> [args...]
#
# Commands:
#   latency    <iface> <delay_ms> <loss_pct>    Inject netem latency + packet loss
#   throttle   <iface> <rate_mbit>              Limit bandwidth with TBF shaper
#   corrupt    <iface> <corrupt_pct>            Add netem packet corruption
#   partition  <remote_ip>                      DROP all traffic to/from a remote IP
#   block-port <remote_ip> <proto> <port>       DROP a specific port to/from a remote IP
#   clear-netem   <iface>                       Remove all tc rules on an interface
#   clear-iptables <remote_ip>                  Remove DROP rules for a remote IP
#   clear-all                                   Remove ALL tc and iptables DROP rules on this node
#   status                                      Show active tc rules and iptables DROP entries
#
# All operations are non-persistent (lost on reboot).

set -euo pipefail

PROG="$(basename "$0")"

usage() {
    sed -n '/^# Usage/,/^$/{ s/^# //; p }' "$0"
    exit 1
}

require_root() {
    [[ $EUID -eq 0 ]] || { echo "ERROR: must run as root (use sudo)"; exit 1; }
}

# ── latency ──────────────────────────────────────────────────────────────────
cmd_latency() {
    local iface="${1:?Usage: latency <iface> <delay_ms> <loss_pct>}"
    local delay="${2:?}"
    local loss="${3:?}"
    echo "[glitch] Injecting ${delay}ms delay + ${loss}% loss on ${iface}"
    if tc qdisc show dev "${iface}" | grep -q netem; then
        tc qdisc change dev "${iface}" root netem \
            delay "${delay}ms" 50ms distribution normal \
            loss "${loss}%"
    else
        tc qdisc add dev "${iface}" root netem \
            delay "${delay}ms" 50ms distribution normal \
            loss "${loss}%"
    fi
    tc qdisc show dev "${iface}"
}

# ── throttle ─────────────────────────────────────────────────────────────────
cmd_throttle() {
    local iface="${1:?Usage: throttle <iface> <rate_mbit>}"
    local rate="${2:?}"
    echo "[glitch] Throttling ${iface} to ${rate}mbit"
    if tc qdisc show dev "${iface}" | grep -q tbf; then
        tc qdisc change dev "${iface}" root tbf \
            rate "${rate}mbit" burst 32kbit latency 400ms
    else
        tc qdisc add dev "${iface}" root tbf \
            rate "${rate}mbit" burst 32kbit latency 400ms
    fi
    tc qdisc show dev "${iface}"
}

# ── corrupt ───────────────────────────────────────────────────────────────────
cmd_corrupt() {
    local iface="${1:?Usage: corrupt <iface> <corrupt_pct>}"
    local pct="${2:?}"
    echo "[glitch] Injecting ${pct}% packet corruption on ${iface}"
    if tc qdisc show dev "${iface}" | grep -q netem; then
        tc qdisc change dev "${iface}" root netem corrupt "${pct}%"
    else
        tc qdisc add dev "${iface}" root netem corrupt "${pct}%"
    fi
    tc qdisc show dev "${iface}"
}

# ── partition ─────────────────────────────────────────────────────────────────
cmd_partition() {
    local remote="${1:?Usage: partition <remote_ip>}"
    echo "[glitch] Partitioning: DROP all traffic to/from ${remote}"
    iptables -I INPUT  -s "${remote}" -j DROP
    iptables -I OUTPUT -d "${remote}" -j DROP
    echo "[glitch] Active DROP rules for ${remote}:"
    iptables -L -n --line-numbers | grep "${remote}"
}

# ── block-port ────────────────────────────────────────────────────────────────
cmd_block_port() {
    local remote="${1:?Usage: block-port <remote_ip> <proto> <port>}"
    local proto="${2:?}"
    local port="${3:?}"
    echo "[glitch] Blocking ${proto}/${port} to/from ${remote}"
    iptables -I INPUT  -s "${remote}" -p "${proto}" --dport "${port}" -j DROP
    iptables -I OUTPUT -d "${remote}" -p "${proto}" --dport "${port}" -j DROP
    echo "[glitch] Active DROP rules:"
    iptables -L -n --line-numbers | grep -E "${remote}|${port}"
}

# ── clear-netem ───────────────────────────────────────────────────────────────
cmd_clear_netem() {
    local iface="${1:?Usage: clear-netem <iface>}"
    echo "[glitch] Removing tc rules from ${iface}"
    if tc qdisc show dev "${iface}" | grep -qE "netem|tbf"; then
        tc qdisc del dev "${iface}" root
        echo "[glitch] Cleared."
    else
        echo "[glitch] No netem/tbf rules found on ${iface} — nothing to remove."
    fi
    tc qdisc show dev "${iface}"
}

# ── clear-iptables ────────────────────────────────────────────────────────────
cmd_clear_iptables() {
    local remote="${1:?Usage: clear-iptables <remote_ip>}"
    echo "[glitch] Removing all DROP rules for ${remote}"
    # Loop over both INPUT and OUTPUT; remove all matching rules
    for chain in INPUT OUTPUT; do
        while iptables -L "${chain}" -n | grep -q "${remote}"; do
            rule_num=$(iptables -L "${chain}" -n --line-numbers \
                       | grep "${remote}" | head -1 | awk '{print $1}')
            iptables -D "${chain}" "${rule_num}"
        done
    done
    echo "[glitch] Done. Remaining rules mentioning ${remote}:"
    iptables -L -n | grep "${remote}" || echo "  (none)"
}

# ── clear-all ─────────────────────────────────────────────────────────────────
cmd_clear_all() {
    echo "[glitch] Removing all injected tc faults on all interfaces"
    for iface in $(ip -o link show | awk -F': ' '{print $2}' | grep -v lo); do
        if tc qdisc show dev "${iface}" | grep -qE "netem|tbf"; then
            echo "  clearing ${iface}"
            tc qdisc del dev "${iface}" root 2>/dev/null || true
        fi
    done

    echo "[glitch] Removing all injected iptables DROP rules"
    # Flush only DROP rules from INPUT and OUTPUT chains (safer than -F)
    for chain in INPUT OUTPUT; do
        iptables -L "${chain}" -n --line-numbers \
            | awk '/DROP/{print $1}' \
            | sort -rn \
            | while read -r num; do
                iptables -D "${chain}" "${num}" 2>/dev/null || true
              done
    done

    echo "[glitch] All faults cleared."
}

# ── status ────────────────────────────────────────────────────────────────────
cmd_status() {
    echo "=== Active tc rules ==="
    for iface in $(ip -o link show | awk -F': ' '{print $2}' | grep -v lo); do
        result=$(tc qdisc show dev "${iface}" | grep -Ev "^qdisc (mq|fq_codel|noqueue|pfifo_fast)")
        if [[ -n "${result}" ]]; then
            echo "  ${iface}: ${result}"
        fi
    done

    echo ""
    echo "=== Active iptables DROP rules (INPUT) ==="
    iptables -L INPUT -n -v --line-numbers | grep DROP || echo "  (none)"

    echo ""
    echo "=== Active iptables DROP rules (OUTPUT) ==="
    iptables -L OUTPUT -n -v --line-numbers | grep DROP || echo "  (none)"
}

# ── dispatch ──────────────────────────────────────────────────────────────────
require_root

CMD="${1:-}"
shift || true

case "${CMD}" in
    latency)       cmd_latency       "$@" ;;
    throttle)      cmd_throttle      "$@" ;;
    corrupt)       cmd_corrupt       "$@" ;;
    partition)     cmd_partition     "$@" ;;
    block-port)    cmd_block_port    "$@" ;;
    clear-netem)   cmd_clear_netem   "$@" ;;
    clear-iptables) cmd_clear_iptables "$@" ;;
    clear-all)     cmd_clear_all     ;;
    status)        cmd_status        ;;
    *)
        echo "ERROR: unknown command '${CMD}'"
        echo ""
        echo "Available commands:"
        echo "  latency    <iface> <delay_ms> <loss_pct>"
        echo "  throttle   <iface> <rate_mbit>"
        echo "  corrupt    <iface> <corrupt_pct>"
        echo "  partition  <remote_ip>"
        echo "  block-port <remote_ip> <proto> <port>"
        echo "  clear-netem   <iface>"
        echo "  clear-iptables <remote_ip>"
        echo "  clear-all"
        echo "  status"
        exit 1
        ;;
esac
