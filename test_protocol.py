from shared.protocol import Message, MessageType


def test_message_roundtrip():
    msg = Message(MessageType.HEARTBEAT, {"client_ts": 123.456})
    line = msg.to_line()
    restored = Message.from_line(line)
    assert restored.type == MessageType.HEARTBEAT
    assert restored.payload["client_ts"] == 123.456
    assert restored.msg_id == msg.msg_id


def test_message_line_is_newline_terminated():
    msg = Message(MessageType.TASK_ASSIGN, {"chunk_id": 1})
    line = msg.to_line()
    assert line.endswith(b"\n")
    assert line.count(b"\n") == 1


def test_unknown_fields_default_sensibly():
    raw = b'{"type": "PAUSE"}\n'
    msg = Message.from_line(raw)
    assert msg.type == "PAUSE"
    assert msg.payload == {}
