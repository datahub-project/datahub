"""Progress tracking for chat UI reasoning steps."""

import time
from dataclasses import dataclass, field
from typing import Optional


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string."""
    if seconds < 1:
        return f"{int(seconds * 1000)}ms"
    elif seconds < 60:
        return f"{int(seconds)}s"
    else:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"


def markdown_to_html(text: str) -> str:
    """Convert simple markdown to HTML (handles **bold**)."""
    import re
    # Convert **text** to <b>text</b>
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    return text


@dataclass
class Step:
    """Represents a single execution step."""
    text: str
    icon: str  # ▶, ✓, ⏳, ✗
    start_time: float
    end_time: Optional[float] = None
    order: int = 0

    @property
    def duration(self) -> Optional[float]:
        """Calculate step duration."""
        if self.end_time:
            return self.end_time - self.start_time
        return None

    @property
    def is_complete(self) -> bool:
        """Check if step is complete."""
        return self.icon == "✓"


class ProgressTracker:
    """Tracks progress of reasoning steps for a chat message."""

    def __init__(self):
        self.start_time = time.time()
        self.plan_name: Optional[str] = None
        self.steps: dict[str, Step] = {}
        self.step_order: list[str] = []  # Maintain insertion order

    def set_plan(self, plan_name: str) -> None:
        """Set the current plan name."""
        self.plan_name = plan_name

    def update_step(self, step_text: str, icon: str) -> bool:
        """
        Update a step's status.

        Returns True if this is a meaningful update (should trigger re-render).
        """
        current_time = time.time()

        if step_text not in self.steps:
            # New step
            order = len(self.step_order)
            self.steps[step_text] = Step(
                text=step_text,
                icon=icon,
                start_time=current_time,
                order=order
            )
            self.step_order.append(step_text)
            return True  # New step is meaningful

        # Existing step - check if icon changed
        step = self.steps[step_text]
        if step.icon != icon:
            # Icon changed (e.g., ▶ -> ✓)
            old_icon = step.icon
            step.icon = icon

            # Mark completion time if transitioning to completed
            if icon == "✓" and old_icon != "✓":
                step.end_time = current_time

            return True  # Icon change is meaningful

        return False  # No meaningful change

    def get_elapsed_time(self) -> float:
        """Get total elapsed time since tracking started."""
        return time.time() - self.start_time

    def render(self) -> list[str]:
        """
        Render current progress as a list of formatted strings.

        Returns list of lines to display.
        """
        lines = []

        # Add Thinking header with overall stats
        elapsed = self.get_elapsed_time()
        message_count = len(self.steps)
        lines.append(f"Thinking ({format_duration(elapsed)}, {message_count} messages)")

        # Add plan header if exists (convert markdown to HTML)
        if self.plan_name:
            plan_html = markdown_to_html(self.plan_name)
            lines.append(f"Plan: {plan_html}")

        # Add steps in order with hierarchical indentation
        for step_text in self.step_order:
            step = self.steps[step_text]
            duration_str = ""

            # Show duration for completed steps
            if step.duration is not None:
                duration_str = f" ({format_duration(step.duration)})"

            lines.append(f"  -- {step.icon} {step.text}{duration_str}")

        return lines

    def render_summary(self) -> str:
        """Render a one-line summary for status labels."""
        elapsed = self.get_elapsed_time()
        step_count = len(self.steps)

        if self.plan_name:
            return f"💭 {self.plan_name} ({step_count} steps, {format_duration(elapsed)})"
        else:
            return f"💭 Thinking... ({step_count} steps, {format_duration(elapsed)})"
