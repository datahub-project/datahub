"""Ask Claude which engineering team should own a PR or issue.

Reads the routing prompt from stdin and prints the model's text answer (a team
key such as ING, or NONE) to stdout. Prints nothing and exits 0 if the call
fails, so the calling workflow falls back to its default team instead of
failing the job.

Authentication comes from the Anthropic SDK's own credential resolution. In CI
that is Workload Identity Federation (ANTHROPIC_FEDERATION_RULE_ID,
ANTHROPIC_ORGANIZATION_ID, ANTHROPIC_SERVICE_ACCOUNT_ID, ANTHROPIC_WORKSPACE_ID
and ANTHROPIC_IDENTITY_TOKEN_FILE), so no static API key is needed. Do not set
ANTHROPIC_API_KEY: it outranks federation.
"""

import sys

MODEL = "claude-haiku-5-5"


def main() -> int:
    prompt = sys.stdin.read()
    try:
        # Imported inside the try so a failed SDK install degrades to the
        # workflow's fallback team rather than a traceback.
        import anthropic

        client = anthropic.Anthropic()
        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            output_config={"effort": "low"},
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:
        # stdout is captured by the workflow, so warn on stderr.
        print(f"::warning::Routing LLM call failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 0

    text = next((block.text for block in response.content if block.type == "text"), "")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
