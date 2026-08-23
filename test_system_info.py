from shared import system_info


def test_snapshot_never_raises_and_has_expected_shape():
    snap = system_info.get_full_snapshot()
    assert set(snap.keys()) == {"cpu", "ram", "gpu"}
    assert "usage_percent" in snap["cpu"]
    assert "used_percent" in snap["ram"]
    assert "name" in snap["gpu"]


def test_cpu_info_never_raises():
    info = system_info.get_cpu_info()
    assert isinstance(info, dict)


def test_ram_info_never_raises():
    info = system_info.get_ram_info()
    assert isinstance(info, dict)


def test_gpu_info_never_raises():
    info = system_info.get_gpu_info()
    assert isinstance(info, dict)
