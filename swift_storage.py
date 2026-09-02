import json
import logging
from typing import Optional, Dict, Any, List, Tuple
from swiftclient.client import Connection, ClientException

from config import (
    SWIFT_AUTH_URL,
    SWIFT_AUTH_VERSION,
    SWIFT_USER,
    SWIFT_KEY,
    SWIFT_CONTAINER,
    SWIFT_ENABLED,
)

logger = logging.getLogger("flowfirst.swift")


def get_swift_connection(
    authurl: Optional[str] = None,
    user: Optional[str] = None,
    key: Optional[str] = None,
    auth_version: Optional[str] = None,
    retries: int = 3,
) -> Connection:
    """
    Establish and return a Swift connection using python-swiftclient.
    Defaults to config settings loaded from environment variables.
    """
    authurl = authurl or SWIFT_AUTH_URL
    user = user or SWIFT_USER
    key = key or SWIFT_KEY
    auth_version = auth_version or SWIFT_AUTH_VERSION

    return Connection(
        authurl=authurl,
        user=user,
        key=key,
        auth_version=auth_version,
        retries=retries,
    )


def ensure_container_exists(container_name: Optional[str] = None) -> bool:
    """
    Ensure the target Swift container exists; creates it if it does not.
    """
    if not SWIFT_ENABLED:
        return False
    container = container_name or SWIFT_CONTAINER
    try:
        conn = get_swift_connection()
        conn.put_container(container)
        return True
    except Exception as e:
        logger.warning(f"Could not ensure container '{container}' exists: {e}")
        return False


def put_message_object(
    message_data: Dict[str, Any],
    container_name: Optional[str] = None,
    object_name: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    Store message data as a JSON object in Swift object storage.
    Object name defaults to '{flow_name}/{message_id}.json' or '{message_id}.json'.
    Returns (success: bool, object_name_or_error: str).
    """
    if not SWIFT_ENABLED:
        return False, "Swift storage is disabled"

    container = container_name or SWIFT_CONTAINER
    msg_id = message_data.get("message_id", "")
    flow = message_data.get("flow", "unknown")
    obj_name = object_name or f"flow{flow}/{msg_id}.json"

    try:
        conn = get_swift_connection()
        # Ensure container exists (idempotent in Swift)
        conn.put_container(container)

        content = json.dumps(message_data, indent=2).encode("utf-8")
        headers = {
            "content-type": "application/json",
            "x-object-meta-flow": str(flow),
            "x-object-meta-message-id": str(msg_id),
            "x-object-meta-item-id": str(message_data.get("item_id", "")),
        }

        conn.put_object(
            container,
            obj_name,
            contents=content,
            content_type="application/json",
            headers=headers,
        )
        return True, obj_name
    except Exception as e:
        logger.error(f"Failed to put object '{obj_name}' in container '{container}': {e}")
        return False, str(e)


def get_message_object(
    object_name: str,
    container_name: Optional[str] = None,
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
    """
    Retrieve and parse a JSON message object from Swift object storage.
    Returns (success: bool, data: Optional[Dict], headers: Optional[Dict]).
    """
    if not SWIFT_ENABLED:
        return False, None, {"error": "Swift storage is disabled"}

    container = container_name or SWIFT_CONTAINER
    try:
        conn = get_swift_connection()
        headers, content = conn.get_object(container, object_name)
        data = json.loads(content.decode("utf-8"))
        return True, data, headers
    except ClientException as ce:
        return False, None, {"error": f"Swift ClientException {ce.http_status}: {ce.http_reason}"}
    except Exception as e:
        return False, None, {"error": str(e)}


def list_message_objects(
    prefix: Optional[str] = None,
    limit: int = 100,
    container_name: Optional[str] = None,
) -> Tuple[bool, List[Dict[str, Any]], Optional[str]]:
    """
    List message objects in the container with optional prefix filtering.
    Returns (success: bool, object_list: List[Dict], error_message: Optional[str]).
    """
    if not SWIFT_ENABLED:
        return False, [], "Swift storage is disabled"

    container = container_name or SWIFT_CONTAINER
    try:
        conn = get_swift_connection()
        _, objects = conn.get_container(container, prefix=prefix, limit=limit)
        return True, objects, None
    except Exception as e:
        return False, [], str(e)


def get_swift_status(container_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Check Swift connectivity and retrieve container metadata.
    """
    if not SWIFT_ENABLED:
        return {"enabled": False, "status": "disabled"}

    container = container_name or SWIFT_CONTAINER
    try:
        conn = get_swift_connection()
        acc_headers, _ = conn.get_account()
        cont_headers = {}
        try:
            cont_headers, _ = conn.get_container(container, limit=1)
        except Exception:
            pass

        return {
            "enabled": True,
            "status": "connected",
            "auth_url": SWIFT_AUTH_URL,
            "container": container,
            "account_objects": acc_headers.get("x-account-object-count", 0),
            "account_bytes": acc_headers.get("x-account-bytes-used", 0),
            "container_headers": cont_headers,
        }
    except Exception as e:
        return {
            "enabled": True,
            "status": "error",
            "auth_url": SWIFT_AUTH_URL,
            "container": container,
            "error": str(e),
        }
