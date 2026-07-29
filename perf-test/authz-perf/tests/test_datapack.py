from unittest.mock import patch

from lib.datapack import LoadAction, load_pack, resolve_load_action


@patch("lib.datapack.subprocess.check_call")
def test_load_pack_injects_gms_env(mock_call, tmp_path) -> None:
    datapack_dir = tmp_path / "pack"
    datapack_dir.mkdir()
    (datapack_dir / "index.json").write_text('{"version": "1"}', encoding="utf-8")

    load_pack(
        gms_url="http://remote:8080",
        token="secret-token",
        datapack_dir=datapack_dir,
    )

    _, kwargs = mock_call.call_args
    assert kwargs["env"]["DATAHUB_GMS_URL"] == "http://remote:8080"
    assert kwargs["env"]["DATAHUB_GMS_TOKEN"] == "secret-token"


def test_forward_upgrade_loads() -> None:
    with patch("lib.datapack.get_loaded_version", return_value="4"):
        decision = resolve_load_action(
            "http://localhost:8080",
            "5",
            sentinel_present=True,
        )
        assert decision.action == LoadAction.LOAD


def test_skip_load_flag() -> None:
    decision = resolve_load_action(
        "http://localhost:8080",
        "5",
        skip_load=True,
        sentinel_present=True,
    )
    assert decision.action == LoadAction.SKIP
