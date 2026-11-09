"""Workaround for CVE-2019-25781 / GHSA-4g9h-pv2v-fw83 (Python-Markdown issue #798).

Python-Markdown's link and image inline processors are quadratic on unclosed brackets
(e.g. ``"[" * 32000`` takes minutes). This extension deregisters them for the
datahub-documents partitioner only, so link and image syntax stays as literal text.

Removal, once a Python-Markdown release fixes #798 (verify first:
``markdown.markdown("[" * 32000)`` should finish in under a second):

1. Floor ``markdown>=<fixed version>`` in ``unstructured_lib`` in ``setup.py`` (or bump
   ``unstructured[md]`` if its minimum already requires the fixed version).
2. Delete this module and its tests.
3. Delete the ``disable_markdown_link_parsing`` config field (deprecate it first, e.g. with
   ``pydantic_removed_field``, so recipes that set it keep validating), the flag branch in
   ``TextPartitioner``, and the ``markdown_link_parsing`` fingerprint key.
4. Keep ``partition_timeout_seconds``; it is not part of this workaround.
5. Deleting the flag does NOT change the processing fingerprint at its default, so nothing
   is re-embedded: documents embedded during the workaround keep link syntax as literal
   text until they are edited. For a consistent index, deliberately add a new fingerprint
   value in the removal PR (re-embeds every document once) and note it in the release notes.
"""

from markdown import Markdown
from markdown.extensions import Extension

# Every inline pattern that calls LinkInlineProcessor.getText()/getLink()
# (registered in markdown/inlinepatterns.py build_inlinepatterns()).
_LINK_PATTERNS = (
    "reference",
    "link",
    "image_link",
    "image_reference",
    "short_reference",
    "short_image_ref",
)


class NoInlineLinksExtension(Extension):
    """Deregister Python-Markdown's link and image inline patterns."""

    def extendMarkdown(self, md: Markdown) -> None:
        for name in _LINK_PATTERNS:
            if name in md.inlinePatterns:
                md.inlinePatterns.deregister(name)
