import pytest

from node_agent.protocol import parse_legacy_request


def test_parse_legacy_request_accepts_supported_minor_window():
    envelope = parse_legacy_request(
        {
            "type": "task_cancel",
            "task_id": "t1",
            "protocol_version": "v1.0",
        }
    )
    assert envelope.task_cancel is not None
    assert envelope.task_cancel.task_id == "t1"


def test_parse_legacy_request_rejects_unsupported_major_version():
    with pytest.raises(ValueError, match="仅兼容 v1.x"):
        parse_legacy_request(
            {
                "type": "task_cancel",
                "task_id": "t1",
                "protocol_version": "v2.0",
            }
        )


def test_parse_legacy_request_rejects_too_old_minor_version():
    with pytest.raises(ValueError, match="超出兼容窗口"):
        parse_legacy_request(
            {
                "type": "task_cancel",
                "task_id": "t1",
                "protocol_version": "v1.-1",
            }
        )
