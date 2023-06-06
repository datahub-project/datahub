from typing import Mapping
from urllib import parse as urlparse

from jsonref import JsonRef, _replace_refs


def title_swapping_callback(self: JsonRef) -> dict:
    """A patched version of jsonref.JsonRef.callback which adds a title with the reference name"""
    # Copy-paste of jsonref.JsonRef.callback
    uri, fragment = urlparse.urldefrag(self.full_uri)

    # If we already looked this up, return a reference to the same object
    if uri not in self.store:
        # Remote ref
        try:
            base_doc = self.loader(uri)
        except Exception as e:
            raise self._error("%s: %s" % (e.__class__.__name__, str(e)), cause=e) from e
        base_doc = _replace_refs(
            base_doc, **{**self._ref_kwargs, "base_uri": uri, "recursing": False}
        )
    else:
        base_doc = self.store[uri]
    result = self.resolve_pointer(base_doc, fragment)
    if result is self:
        raise self._error("Reference refers directly to itself.")
    if hasattr(result, "__subject__"):
        result = result.__subject__
    if self.merge_props and isinstance(result, Mapping) and len(self.__reference__) > 1:
        result = {
            **result,
            **{k: v for k, v in self.__reference__.items() if k != "$ref"},
        }

    # The only modifications (additions) to jsonref.JsonRef.callback follow
    if ("title" not in result) and fragment:
        result["title"] = fragment.split("/")[-1]
    return result
