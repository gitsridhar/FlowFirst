#!/usr/bin/env bash
# tcpdump_flows.sh — automate packet capture, pipeline flow execution, and round-trip analysis
#
# Usage:
#   sudo ./scripts/tcpdump_flows.sh start  [iface]           Start background capture on this node
#   sudo ./scripts/tcpdump_flows.sh run-flows [vip_ip]        Trigger Flow1 + Flow2 via REST API
#   sudo ./scripts/tcpdump_flows.sh stop                      Stop the background capture
#   sudo ./scripts/tcpdump_flows.sh analyse [pcap_file]       Print round-trip summary for all protocols
#   sudo ./scripts/tcpdump_flows.sh all    [iface] [vip_ip]   start + run-flows + stop + analyse
#
# Requires: tcpdump, tshark (wireshark-cli), curl, jq, awk, sed

set -euo pipefail

PROG="$(basename "$0")"
PCAP_DIR=/var/log/flowfirst/pcap
PID_FILE=/var/run/tcpdump_flowfirst.pid
DEFAULT_IFACE=eth0
DEFAULT_VIP=192.168.1.100

# Capture filter covering all FlowFirst cluster protocols
CAPTURE_FILTER='port 5672 or port 25672 or port 5405 or port 4567 or port 4568 or port 4444 or port 3306 or port 8080 or port 9000'

# ── helpers ───────────────────────────────────────────────────────────────────
require_root() {
    [[ $EUID -eq 0 ]] || { echo "ERROR: run as root (sudo)"; exit 1; }
}

hr() { printf '%0.s─' $(seq 1 70); echo; }

info()  { echo "[tcpdump_flows] $*"; }
ok()    { echo "  ✅ $*"; }
warn()  { echo "  ⚠️  $*"; }
fail()  { echo "  ❌ $*"; }

count_or_zero() {
    # safely count grep output lines; returns 0 if grep finds nothing
    grep -c "$1" 2>/dev/null || true
}

# ── start ─────────────────────────────────────────────────────────────────────
cmd_start() {
    local iface="${1:-${DEFAULT_IFACE}}"
    require_root
    sudo mkdir -p "${PCAP_DIR}"

    local node
    node=$(hostname -s)
    local pcap="${PCAP_DIR}/${node}_$(date +%Y%m%d_%H%M%S).pcap"

    info "Starting capture on ${iface} → ${pcap}"
    tcpdump -i "${iface}" -s 0 -Z root \
        -w "${pcap}" \
        "${CAPTURE_FILTER}" &
    local pid=$!
    echo "${pid}" > "${PID_FILE}"
    echo "${pcap}" > "${PID_FILE}.pcap"
    info "tcpdump PID=${pid}  pcap=${pcap}"
}

# ── run-flows ─────────────────────────────────────────────────────────────────
cmd_run_flows() {
    local vip="${1:-${DEFAULT_VIP}}"
    info "Triggering Flow1 via http://${vip}:8080/api/flow1"
    curl -s -X POST "http://${vip}:8080/api/flow1" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print('  flow1 → handled_by_node:', d.get('handled_by_node','?'))" \
        2>/dev/null || warn "flow1 request failed — is the API running?"

    info "Triggering Flow2 via http://${vip}:8080/api/flow2"
    curl -s -X POST "http://${vip}:8080/api/flow2" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print('  flow2 → handled_by_node:', d.get('handled_by_node','?'))" \
        2>/dev/null || warn "flow2 request failed"

    info "Triggering /health (round-robin probe)"
    for i in 1 2 3; do
        node=$(curl -s "http://${vip}:8080/health" | \
               python3 -c "import sys,json; print(json.load(sys.stdin).get('handled_by_node','?'))" 2>/dev/null || echo "?")
        echo "  probe ${i} → ${node}"
        sleep 0.5
    done

    info "Waiting 10 s for full pipeline traversal ..."
    sleep 10
    info "Done."
}

# ── stop ──────────────────────────────────────────────────────────────────────
cmd_stop() {
    if [[ -f "${PID_FILE}" ]]; then
        local pid
        pid=$(cat "${PID_FILE}")
        info "Stopping tcpdump PID=${pid}"
        kill "${pid}" 2>/dev/null || true
        sleep 1
        rm -f "${PID_FILE}"
        local pcap
        pcap=$(cat "${PID_FILE}.pcap" 2>/dev/null || echo "")
        rm -f "${PID_FILE}.pcap"
        info "Capture stopped. pcap file: ${pcap}"
        echo "${pcap}"
    else
        warn "No PID file found at ${PID_FILE} — capture may already be stopped."
        ls -1t "${PCAP_DIR}"/*.pcap 2>/dev/null | head -1 || true
    fi
}

# ── analyse ───────────────────────────────────────────────────────────────────
cmd_analyse() {
    local pcap="${1:-}"
    if [[ -z "${pcap}" ]]; then
        pcap=$(ls -1t "${PCAP_DIR}"/*.pcap 2>/dev/null | head -1 || true)
        [[ -n "${pcap}" ]] || { echo "ERROR: no pcap file found in ${PCAP_DIR}"; exit 1; }
    fi
    [[ -f "${pcap}" ]] || { echo "ERROR: pcap file not found: ${pcap}"; exit 1; }

    info "Analysing: ${pcap}"
    hr

    # ── 1. AMQP TCP three-way handshakes ─────────────────────────────────────
    echo "1. AMQP TCP Three-Way Handshakes (port 5672)"
    local syns syn_acks
    syns=$(tcpdump -r "${pcap}" -nn 'port 5672' 2>/dev/null | grep -c "Flags \[S\b" || true)
    syn_acks=$(tcpdump -r "${pcap}" -nn 'port 5672' 2>/dev/null | grep -c "Flags \[S\.\]" || true)
    echo "   SYN packets   : ${syns}"
    echo "   SYN-ACK packets: ${syn_acks}"
    if [[ "${syns}" -gt 0 && "${syn_acks}" -eq "${syns}" ]]; then
        ok "Every SYN has a matching SYN-ACK — TCP handshake complete"
    elif [[ "${syns}" -eq 0 ]]; then
        warn "No AMQP connections captured — are processes running?"
    else
        fail "SYN count (${syns}) ≠ SYN-ACK count (${syn_acks}) — some connections failed"
    fi
    hr

    # ── 2. AMQP method sequence ───────────────────────────────────────────────
    echo "2. AMQP Method Sequence (tshark)"
    if command -v tshark &>/dev/null; then
        local open_ok publish deliver ack
        open_ok=$(tshark -r "${pcap}" -d tcp.port==5672,amqp \
            -Y 'amqp.method.method == "connection.open-ok"' \
            -T fields -e frame.number 2>/dev/null | wc -l)
        publish=$(tshark -r "${pcap}" -d tcp.port==5672,amqp \
            -Y 'amqp.method.method == "basic.publish"' \
            -T fields -e frame.number 2>/dev/null | wc -l)
        deliver=$(tshark -r "${pcap}" -d tcp.port==5672,amqp \
            -Y 'amqp.method.method == "basic.deliver"' \
            -T fields -e frame.number 2>/dev/null | wc -l)
        ack=$(tshark -r "${pcap}" -d tcp.port==5672,amqp \
            -Y 'amqp.method.method == "basic.ack"' \
            -T fields -e frame.number 2>/dev/null | wc -l)
        echo "   connection.open-ok : ${open_ok}"
        echo "   basic.publish      : ${publish}"
        echo "   basic.deliver      : ${deliver}"
        echo "   basic.ack          : ${ack}"
        [[ "${open_ok}" -ge 1 ]] && ok "AMQP connections negotiated (open-ok present)" \
                                  || fail "No connection.open-ok — AMQP negotiation incomplete"
        [[ "${publish}" -ge 1 ]] && ok "Messages published (basic.publish present)" \
                                  || warn "No basic.publish found"
        [[ "${ack}" -ge "${publish}" ]] && ok "All publishes acknowledged (acks=${ack} >= publishes=${publish})" \
                                        || warn "Some messages unacknowledged (acks=${ack} < publishes=${publish})"
    else
        warn "tshark not installed — skipping AMQP method decode (install wireshark-cli)"
    fi
    hr

    # ── 3. AMQP RST check ────────────────────────────────────────────────────
    echo "3. AMQP Abrupt Termination Check (RST packets)"
    local rsts
    rsts=$(tcpdump -r "${pcap}" -nn 'port 5672 and tcp[tcpflags] & tcp-rst != 0' \
        2>/dev/null | wc -l)
    echo "   RST packets: ${rsts}"
    [[ "${rsts}" -eq 0 ]] && ok "No RST packets — all connections closed gracefully" \
                           || fail "${rsts} RST packet(s) detected — abnormal connection termination"
    hr

    # ── 4. AMQP FIN teardown ─────────────────────────────────────────────────
    echo "4. AMQP Graceful Teardown (FIN packets)"
    local fins
    fins=$(tcpdump -r "${pcap}" -nn 'port 5672 and tcp[tcpflags] & tcp-fin != 0' \
        2>/dev/null | wc -l)
    echo "   FIN packets: ${fins}"
    [[ "${fins}" -ge 2 ]] && ok "Graceful FIN teardown observed" \
                           || warn "No FIN packets — connections may still be open (normal for long-lived connections)"
    hr

    # ── 5. Corosync heartbeats ────────────────────────────────────────────────
    echo "5. Corosync Heartbeat (UDP 5405)"
    local corosync_total
    corosync_total=$(tcpdump -r "${pcap}" -nn 'udp port 5405' 2>/dev/null | wc -l)
    echo "   Total Corosync UDP packets: ${corosync_total}"
    if [[ "${corosync_total}" -gt 0 ]]; then
        echo "   Packets per source IP:"
        tcpdump -r "${pcap}" -nn 'udp port 5405' 2>/dev/null \
            | awk '{print $3}' | cut -d. -f1-4 | sort | uniq -c | sort -rn \
            | while read -r cnt ip; do echo "     ${ip}  →  ${cnt} packets"; done
        local pairs
        pairs=$(tcpdump -r "${pcap}" -nn 'udp port 5405' 2>/dev/null \
            | awk '{print $3, $5}' | sed 's/\.[0-9]*://g' | sort -u | wc -l)
        echo "   Unique src→dst pairs: ${pairs}"
        [[ "${pairs}" -ge 6 ]] && ok "All 6 directed node pairs communicating" \
                                || warn "Only ${pairs}/6 directed pairs — some nodes may be isolated"
    else
        warn "No Corosync packets — is Pacemaker running?"
    fi
    hr

    # ── 6. Galera replication ─────────────────────────────────────────────────
    echo "6. Galera wsrep Replication (TCP 4567)"
    local galera_syns
    galera_syns=$(tcpdump -r "${pcap}" -nn 'tcp port 4567 and tcp[tcpflags] & tcp-syn != 0' \
        2>/dev/null | grep -c "Flags \[S\b" || true)
    echo "   Galera SYN packets: ${galera_syns}"
    local galera_data
    galera_data=$(tcpdump -r "${pcap}" -nn 'tcp port 4567' 2>/dev/null | wc -l)
    echo "   Galera total TCP segments: ${galera_data}"
    if [[ "${galera_syns}" -ge 2 ]]; then
        ok "Galera TCP connections established (SYN count=${galera_syns})"
    elif [[ "${galera_data}" -gt 0 ]]; then
        ok "Galera replication traffic present (established connections, no new SYNs needed)"
    else
        warn "No Galera traffic — check MariaDB cluster status"
    fi
    hr

    # ── 7. Galera retransmissions ─────────────────────────────────────────────
    echo "7. Galera TCP Retransmissions"
    if command -v tshark &>/dev/null; then
        local retrans
        retrans=$(tshark -r "${pcap}" \
            -Y 'tcp.analysis.retransmission and tcp.port==4567' \
            -T fields -e frame.number 2>/dev/null | wc -l)
        echo "   Retransmitted segments: ${retrans}"
        [[ "${retrans}" -eq 0 ]] && ok "No Galera retransmissions — clean replication" \
                                  || warn "${retrans} retransmission(s) — possible network issue"
    else
        warn "tshark not installed — skipping retransmission analysis"
    fi
    hr

    # ── 8. MariaDB round-trip ─────────────────────────────────────────────────
    echo "8. MariaDB Client Round-Trip (TCP 3306)"
    local mysql_segs
    mysql_segs=$(tcpdump -r "${pcap}" -nn 'tcp port 3306' 2>/dev/null | wc -l)
    echo "   MariaDB TCP segments: ${mysql_segs}"
    if command -v tshark &>/dev/null; then
        local queries
        queries=$(tshark -r "${pcap}" -d tcp.port==3306,mysql \
            -Y 'mysql.command == 3' \
            -T fields -e mysql.query 2>/dev/null | wc -l)
        echo "   COM_QUERY requests decoded: ${queries}"
        [[ "${queries}" -ge 1 ]] && ok "MariaDB query round-trip confirmed" \
                                  || { [[ "${mysql_segs}" -gt 0 ]] && ok "MariaDB traffic present (tshark decode limited)" \
                                                                    || warn "No MariaDB traffic captured"; }
    else
        [[ "${mysql_segs}" -gt 0 ]] && ok "MariaDB traffic present" \
                                     || warn "No MariaDB traffic — is process4 running?"
    fi
    hr

    # ── 9. HTTP REST round-trip ───────────────────────────────────────────────
    echo "9. HTTP REST API Round-Trip (TCP 8080)"
    local posts oks
    posts=$(tcpdump -r "${pcap}" -nn -A 'tcp port 8080' 2>/dev/null \
        | grep -c "POST /api/" || true)
    oks=$(tcpdump -r "${pcap}" -nn -A 'tcp port 8080' 2>/dev/null \
        | grep -c "200 OK" || true)
    echo "   POST requests  : ${posts}"
    echo "   200 OK responses: ${oks}"
    if [[ "${posts}" -gt 0 && "${oks}" -eq "${posts}" ]]; then
        ok "Every POST request received a 200 OK"
    elif [[ "${posts}" -eq 0 ]]; then
        warn "No POST requests captured on port 8080"
    else
        fail "Mismatch: ${posts} POSTs but only ${oks} 200 OKs"
    fi
    hr

    # ── 10. TCP initial RTT summary ────────────────────────────────────────────
    echo "10. TCP Initial RTT Summary (tshark)"
    if command -v tshark &>/dev/null; then
        for port in 5672 8080 4567 3306; do
            local rtt
            rtt=$(tshark -r "${pcap}" \
                -Y "tcp.analysis.initial_rtt and tcp.port==${port}" \
                -T fields -e tcp.analysis.initial_rtt 2>/dev/null \
                | awk '{sum+=$1;n++} END{if(n>0) printf "%.3fms (n=%d)", sum*1000/n, n; else print "N/A"}')
            printf "   port %-5s avg RTT: %s\n" "${port}" "${rtt}"
        done
        ok "RTT measurements complete — compare against baseline for glitch scenarios"
    else
        warn "tshark not installed — skipping RTT analysis"
    fi
    hr

    info "Analysis complete for: ${pcap}"
}

# ── all ───────────────────────────────────────────────────────────────────────
cmd_all() {
    local iface="${1:-${DEFAULT_IFACE}}"
    local vip="${2:-${DEFAULT_VIP}}"
    require_root
    cmd_start "${iface}"
    sleep 2    # give tcpdump time to open the interface
    cmd_run_flows "${vip}"
    local pcap
    pcap=$(cmd_stop)
    cmd_analyse "${pcap}"
}

# ── dispatch ──────────────────────────────────────────────────────────────────
CMD="${1:-}"
shift || true

case "${CMD}" in
    start)      cmd_start     "$@" ;;
    run-flows)  cmd_run_flows  "$@" ;;
    stop)       cmd_stop       ;;
    analyse)    cmd_analyse    "$@" ;;
    all)        cmd_all        "$@" ;;
    *)
        echo "Usage: sudo ${PROG} <command> [args...]"
        echo ""
        echo "Commands:"
        echo "  start     [iface]           Start background capture (default: ${DEFAULT_IFACE})"
        echo "  run-flows [vip_ip]          Trigger Flow1 + Flow2 (default VIP: ${DEFAULT_VIP})"
        echo "  stop                        Stop capture"
        echo "  analyse   [pcap_file]       Analyse latest (or specified) pcap"
        echo "  all       [iface] [vip_ip]  start + run-flows + stop + analyse"
        exit 1
        ;;
esac
