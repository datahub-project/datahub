from typing import Optional


def formulate_description(
    display_name: Optional[str], description: Optional[str]
) -> str:
    if display_name and description:
        return f"{display_name}\n-----\n{description}"

    if display_name:
        return display_name

    return ""
