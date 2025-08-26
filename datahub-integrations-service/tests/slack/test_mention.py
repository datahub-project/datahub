from datahub_integrations.slack.command.mention import (
    _build_progress_message,
)


def test_build_progress_message_basic_functionality() -> None:
    """Test basic progress message building with a few steps."""
    steps = ["Loading files", "Processing data", "Finalizing"]
    text, blocks = _build_progress_message(steps)

    assert text == ":hourglass_flowing_sand: _*Finalizing*_"
    assert len(blocks) == 1
    assert blocks[0]["type"] == "context"
    assert len(blocks[0]["elements"]) == 3

    # Check that previous steps show checkmarks and current shows hourglass
    assert ":white_check_mark:" in blocks[0]["elements"][0]["text"]
    assert ":white_check_mark:" in blocks[0]["elements"][1]["text"]
    assert ":hourglass_flowing_sand:" in blocks[0]["elements"][2]["text"]


def test_build_progress_message_limits_to_ten_elements() -> None:
    """Test that more than 10 steps are limited to last 9 previous + current."""
    steps = [f"Step {i}" for i in range(1, 16)]  # 15 steps total
    text, blocks = _build_progress_message(steps)

    assert text == ":hourglass_flowing_sand: _*Step 15*_"
    assert len(blocks[0]["elements"]) == 10

    # Should show steps 6-14 as previous (last 9), and step 15 as current
    assert "Step 6" in blocks[0]["elements"][0]["text"]
    assert "Step 14" in blocks[0]["elements"][8]["text"]
    assert "Step 15" in blocks[0]["elements"][9]["text"]
