#!/usr/bin/env bash
# ==============================================================================
# Pacemaker Cluster Control CLI Helper for FlowFirst
# ==============================================================================

ACTION="${1:-status}"

case "${ACTION}" in
    status)
        sudo pcs status
        ;;
    resources)
        sudo pcs resource status
        ;;
    start)
        echo "Starting all FlowFirst cluster resources..."
        sudo pcs resource enable flowfirst-group
        ;;
    stop)
        echo "Stopping all FlowFirst cluster resources..."
        sudo pcs resource disable flowfirst-group
        ;;
    restart)
        echo "Restarting FlowFirst cluster resources..."
        sudo pcs resource restart flowfirst-group
        ;;
    cleanup)
        echo "Cleaning up resource failcounts and refreshing states..."
        sudo pcs resource cleanup flowfirst-group
        ;;
    logs)
        echo "Showing live cluster logs..."
        sudo journalctl -u pacemaker -u corosync -u 'flowfirst-*' -f
        ;;
    *)
        echo "Usage: $0 {status|resources|start|stop|restart|cleanup|logs}"
        exit 1
        ;;
esac
