from shared import crypto


def test_pairing_code_format():
    code = crypto.generate_pairing_code()
    assert code.isdigit()
    assert len(code) == 6


def test_pairing_codes_are_not_constant():
    codes = {crypto.generate_pairing_code() for _ in range(20)}
    assert len(codes) > 1  # extremely unlikely to collide 20/20 times


def test_sign_verify_roundtrip():
    token = crypto.generate_session_token()
    sig = crypto.sign(token, "hello")
    assert crypto.verify(token, "hello", sig)


def test_verify_rejects_tampered_message():
    token = crypto.generate_session_token()
    sig = crypto.sign(token, "hello")
    assert not crypto.verify(token, "tampered", sig)


def test_verify_rejects_wrong_token():
    token_a = crypto.generate_session_token()
    token_b = crypto.generate_session_token()
    sig = crypto.sign(token_a, "hello")
    assert not crypto.verify(token_b, "hello", sig)
