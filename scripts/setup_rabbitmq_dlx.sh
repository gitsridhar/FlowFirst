#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# FlowFirst - dead-letter policy for the pipeline queues (run ONCE, on any RabbitMQ node, with sudo).
#
#   sudo ./scripts/setup_rabbitmq_dlx.sh            apply the policy
#   sudo ./scripts/setup_rabbitmq_dlx.sh --remove   remove it (rejected messages are discarded again)
#   ./scripts/setup_rabbitmq_dlx.sh --print         show the command and exit (no root needed)
#
# Why: a consumer that fails a message does basic_nack(requeue=False). Without a dead-letter exchange RabbitMQ then
# DISCARDS the message. With this policy it is routed to the fanout exchange "flowfirst.dlx" and kept in the durable
# queue "flowfirst_dead_letters" (both declared by config.setup_queues()), where it can be inspected and replayed with
# scripts/replay_dead_letters.py.
#
# It is a POLICY, not a queue argument, so it can be added to or removed from a running cluster. Changing queue
# arguments instead would make RabbitMQ refuse the declaration (PRECONDITION_FAILED) from every running process.
#
# The pattern ^flow[123]_ matches the nine pipeline queues and deliberately NOT flowfirst_dead_letters, so a message
# that is dead-lettered can never be dead-lettered again in a loop.
#
# rabbitmqctl talks to the node over Erlang distribution, not AMQP, so this works the same with TLS on or off.
# Start the pipeline processes (or call setup_queues) once BEFORE or after applying it; the order does not matter.
# ==============================================================================

POLICY_NAME="flowfirst-dlx"
PATTERN='^flow[123]_'
DEFINITION='{"dead-letter-exchange":"flowfirst.dlx"}'

case "${1:-}" in
    --print)  echo "rabbitmqctl set_policy ${POLICY_NAME} '${PATTERN}' '${DEFINITION}' --apply-to queues"; exit 0 ;;
    --remove) [[ $EUID -eq 0 ]] || { echo "ERROR: run as root (sudo)" >&2; exit 1; }
              rabbitmqctl clear_policy "${POLICY_NAME}"; exit 0 ;;
    "")       ;;
    *)        echo "usage: $0 [--print|--remove]" >&2; exit 2 ;;
esac

[[ $EUID -eq 0 ]] || { echo "ERROR: run as root (sudo)" >&2; exit 1; }
command -v rabbitmqctl >/dev/null 2>&1 || { echo "ERROR: rabbitmqctl not found on this node." >&2; exit 1; }

rabbitmqctl set_policy "${POLICY_NAME}" "${PATTERN}" "${DEFINITION}" --apply-to queues
echo "Policy applied. Check with: sudo rabbitmqctl list_policies"
