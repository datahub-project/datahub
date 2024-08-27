try:
    import markupsafe

    # This monkeypatch hack is required for markupsafe>=2.1.0 and older versions of Jinja2.
    # Changelog: https://markupsafe.palletsprojects.com/en/2.1.x/changes/#version-2-1-0
    # Example discussion: https://github.com/aws/aws-sam-cli/issues/3661.
    markupsafe.soft_unicode = markupsafe.soft_str  # type: ignore[attr-defined]

    MARKUPSAFE_PATCHED = True
except ImportError:
    MARKUPSAFE_PATCHED = False
