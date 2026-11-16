"""Workaround for CVE-2019-25781 / GHSA-4g9h-pv2v-fw83 (Python-Markdown issue #798).

Python-Markdown's link and image inline processors are quadratic on unclosed brackets
(e.g. ``"[" * 32000`` takes minutes). This extension deregisters them for the
datahub-documents partitioner only, so link and image syntax stays as literal text.

Removal, once a Python-Markdown release fixes #798 (verify first:
``markdown.markdown("[" * 32000)`` should finish in under a second):

1. Floor ``markdown>=<fixed version>`` in ``unstructured_lib`` in ``setup.py`` (or bump
   ``unstructured[md]`` if its minimum already requires the fixed version), then run
   ``updateLockFile``.
2. Delete this module and ``TestMarkdownLinkGuard``.
3. In ``TextPartitioner.partition_text``, drop the import and the extensions argument:
   ``partition_md(text=text)``. Keep the time limit around it; it also guards against
   other slow input (e.g. spaCy's tokenizer on long punctuation runs).
4. Documents embedded during the workaround keep link syntax as literal text until they
   are edited. For a consistent index, add a new value to the processing fingerprint in
   ``DataHubDocumentsSource`` (re-embeds every document once) and note it in the release
   notes.
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
