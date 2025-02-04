import re

from datahub.ingestion.source.ms_fabric.tmdl.exceptions import ValidationError


class DAXParser:
    """Parser for DAX expressions."""

    # Key DAX keywords that indicate expression boundaries
    KEYWORDS = {
        "EVALUATE",
        "DEFINE",
        "MEASURE",
        "VAR",
        "RETURN",
        "ORDER",
        "BY",
    }

    def validate(self, expression: str) -> None:
        """Validate DAX expression syntax.

        Args:
            expression: DAX expression to validate

        Raises:
            ValidationError: If expression syntax is invalid
        """
        try:
            # Basic validation - check for balanced parentheses and quotes
            self._validate_balanced_tokens(expression)
            # Check for basic DAX structure
            self._validate_structure(expression)
        except Exception as e:
            raise ValidationError(f"Invalid DAX syntax: {str(e)}")

    def _validate_balanced_tokens(self, expression: str) -> None:
        """Validate balanced parentheses and quotes."""
        stack = []
        in_quote = False

        for char in expression:
            if char == '"' and not in_quote:
                in_quote = True
            elif char == '"' and in_quote:
                in_quote = False
            elif not in_quote:
                if char in "({[":
                    stack.append(char)
                elif char in ")}]":
                    if not stack:
                        raise ValidationError("Unmatched closing bracket/parenthesis")
                    if (
                        (char == ")" and stack[-1] != "(")
                        or (char == "}" and stack[-1] != "{")
                        or (char == "]" and stack[-1] != "[")
                    ):
                        raise ValidationError("Mismatched brackets/parentheses")
                    stack.pop()

        if stack:
            raise ValidationError("Unclosed brackets/parentheses")
        if in_quote:
            raise ValidationError("Unclosed quote")

    def _validate_structure(self, expression: str) -> None:
        """Validate basic DAX expression structure."""
        # Remove comments and normalize whitespace
        cleaned = re.sub(r"//.*$", "", expression, flags=re.MULTILINE)
        cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
        cleaned = cleaned.strip()

        # Check for empty expression
        if not cleaned:
            raise ValidationError("Empty expression")

        # Validate function calls
        self._validate_function_calls(cleaned)

    def _validate_function_calls(self, expression: str) -> None:
        """Validate DAX function calls."""
        # Find all function calls
        func_matches = re.finditer(r"\b[A-Za-z_][A-Za-z0-9_.]*\s*\(", expression)

        for match in func_matches:
            func_name = match.group().strip("( ")
            # Validate function name format
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_.]*$", func_name):
                raise ValidationError(f"Invalid function name: {func_name}")
