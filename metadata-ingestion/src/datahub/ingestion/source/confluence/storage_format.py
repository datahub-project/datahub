import re
from typing import Callable, List, Optional, Pattern, Union

from bs4 import BeautifulSoup, NavigableString, Tag
from markdownify import markdownify
from pydantic import BaseModel, ConfigDict

from datahub.ingestion.source.confluence.constants import (
    DISCARD_MACROS,
    NAMESPACED_TAG_PATTERN,
    TASK_STATUS_COMPLETE,
    UNRESOLVED_USER_TEXT,
    MacroName,
    MacroParam,
    StorageAttr,
    StorageTag,
)

_NAMESPACED_TAG = re.compile(NAMESPACED_TAG_PATTERN)
_CODE_LANGUAGE_ATTR = "data-language"


class UserRef(BaseModel):
    """A user referenced from storage format. Cloud mentions carry an account
    id; Data Center mentions carry a user key, and older Server content a
    username."""

    model_config = ConfigDict(frozen=True)

    account_id: Optional[str] = None
    userkey: Optional[str] = None
    username: Optional[str] = None

    def is_empty(self) -> bool:
        return not (self.account_id or self.userkey or self.username)


UserResolver = Callable[[UserRef], Optional[str]]


def storage_to_markdown(html: str, user_resolver: Optional[UserResolver] = None) -> str:
    """Convert a Confluence ``body.storage`` value to Markdown.

    Storage format keeps the meaning of many elements in attributes (a mention
    is ``<ri:user ri:account-id=...>`` with no text, a date is
    ``<time datetime=...>``), so each such element is rewritten to plain HTML
    before markdownify runs. ``user_resolver`` turns a mention into a display
    name; without one, or when it returns None, a mention becomes ``@user``.
    """
    if not html or not html.strip():
        return ""

    soup = BeautifulSoup(html, "html.parser")
    _StorageRewriter(soup, user_resolver).rewrite()
    markdown = markdownify(
        str(soup),
        heading_style="ATX",
        bullets="-",
        strip=["script", "style"],
        code_language_callback=lambda el: el.get(_CODE_LANGUAGE_ATTR) or None,
    )
    return _normalize_markdown(markdown)


def strip_html_tags(html: str) -> str:
    """Plain-text extraction for when Markdown conversion fails."""
    clean_text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", clean_text).strip()


def _normalize_markdown(text: str) -> str:
    text = text.replace("\r\n", "\n").strip()
    return re.sub(r"\n{3,}", "\n\n", text)


def _attr(tag: Optional[Tag], name: str) -> str:
    if tag is None:
        return ""
    value = tag.get(name)
    return value.strip() if isinstance(value, str) else ""


def _child(tag: Tag, name: str) -> Optional[Tag]:
    found = tag.find(name, recursive=False)
    return found if isinstance(found, Tag) else None


def _param(macro: Tag, name: str) -> str:
    for param in macro.find_all(StorageTag.PARAMETER, recursive=False):
        if isinstance(param, Tag) and _attr(param, StorageAttr.NAME) == name:
            return param.get_text(strip=True)
    return ""


class _StorageRewriter:
    def __init__(self, soup: BeautifulSoup, user_resolver: Optional[UserResolver]):
        self.soup = soup
        self.user_resolver = user_resolver

    def rewrite(self) -> None:
        # Order matters: ADF fallbacks and placeholders are handled before
        # macros move their bodies, and macro parameters are read before the
        # leftover ac:parameter elements are stripped.
        for ext in self._all(StorageTag.ADF_EXTENSION):
            self._rewrite_adf_extension(ext)
        for placeholder in self._all(StorageTag.PLACEHOLDER):
            placeholder.decompose()
        # Innermost macros first, so a nested macro is rewritten before its
        # parent moves or discards it.
        for macro in reversed(self._all(StorageTag.MACRO)):
            self._rewrite_macro(macro)
        for task_list in self._all(StorageTag.TASK_LIST):
            self._rewrite_task_list(task_list)
        for link in self._all(StorageTag.LINK):
            self._rewrite_link(link)
        for image in self._all(StorageTag.IMAGE):
            self._rewrite_image(image)
        for emoticon in self._all(StorageTag.EMOTICON):
            self._replace_with_text(emoticon, self._emoticon_text(emoticon))
        for time_tag in self._all(StorageTag.TIME):
            if not time_tag.get_text(strip=True):
                self._replace_with_text(time_tag, _attr(time_tag, StorageAttr.DATETIME))
        for param in self._all(StorageTag.PARAMETER):
            param.decompose()
        for tag in self._all(_NAMESPACED_TAG):
            tag.unwrap()

    def _all(self, name: Union[str, Pattern[str]]) -> List[Tag]:
        return [
            tag
            for tag in self.soup.find_all(name)
            if isinstance(tag, Tag) and not tag.decomposed
        ]

    def _replace_with_text(self, tag: Tag, text: str) -> None:
        if text:
            tag.replace_with(NavigableString(text))
        else:
            tag.decompose()

    def _rewrite_adf_extension(self, ext: Tag) -> None:
        # Content from the new editor is stored twice: as an ADF node and as an
        # HTML fallback. Keep only the fallback; the node's attributes (panel
        # type, colours) are not text.
        fallback = _child(ext, StorageTag.ADF_FALLBACK)
        if fallback is not None:
            ext.replace_with(fallback)
            fallback.unwrap()
            return
        for attribute in ext.find_all(StorageTag.ADF_ATTRIBUTE):
            attribute.decompose()
        ext.unwrap()

    def _rewrite_macro(self, macro: Tag) -> None:
        if macro.decomposed:
            return
        name = _attr(macro, StorageAttr.NAME).lower()

        if name in DISCARD_MACROS:
            macro.decompose()
        elif name in (MacroName.CODE, MacroName.NOFORMAT):
            self._rewrite_code_macro(macro)
        elif name == MacroName.STATUS:
            title = _param(macro, MacroParam.TITLE)
            self._replace_with_text(macro, f"[{title}]" if title else "")
        elif name == MacroName.JIRA:
            # A single-issue macro has a key; a JQL table renders live results
            # and has nothing authored to keep.
            self._replace_with_text(macro, _param(macro, MacroParam.KEY))
        else:
            # Formatting macros (panel, info, expand, ...) and unknown macros:
            # keep the authored body, drop the wrapper and its parameters.
            body = _child(macro, StorageTag.RICH_TEXT_BODY) or _child(
                macro, StorageTag.PLAIN_TEXT_BODY
            )
            if body is None:
                macro.decompose()
            else:
                macro.replace_with(body)
                body.unwrap()

    def _rewrite_code_macro(self, macro: Tag) -> None:
        language = _param(macro, MacroParam.LANGUAGE)
        body = _child(macro, StorageTag.PLAIN_TEXT_BODY) or _child(
            macro, StorageTag.RICH_TEXT_BODY
        )
        pre = self.soup.new_tag("pre")
        if language:
            pre[_CODE_LANGUAGE_ATTR] = language
        code = self.soup.new_tag("code")
        code.append(NavigableString(body.get_text() if body else ""))
        pre.append(code)
        macro.replace_with(pre)

    def _rewrite_task_list(self, task_list: Tag) -> None:
        ul = self.soup.new_tag("ul")
        for task in task_list.find_all(
            [StorageTag.TASK, StorageTag.TASK_LIST], recursive=False
        ):
            if not isinstance(task, Tag):
                continue
            if task.name == StorageTag.TASK_LIST:
                # A sub-list stored next to its parent task rather than inside
                # its body. Nest it under the previous task; it is rewritten in
                # turn, since every task list is collected before any is rewritten.
                parent_li = ul.contents[-1] if ul.contents else None
                if isinstance(parent_li, Tag):
                    parent_li.append(task.extract())
                else:
                    ul.append(task.extract())
                continue
            status = _child(task, StorageTag.TASK_STATUS)
            done = (
                status is not None
                and status.get_text(strip=True) == TASK_STATUS_COMPLETE
            )
            li = self.soup.new_tag("li")
            li.append(NavigableString("[x] " if done else "[ ] "))
            body = _child(task, StorageTag.TASK_BODY)
            if body is not None:
                li.append(body)
                body.unwrap()
            ul.append(li)
        task_list.replace_with(ul)

    def _rewrite_link(self, link: Tag) -> None:
        user = _child(link, StorageTag.USER)
        if user is not None:
            self._replace_with_text(link, self._mention_text(user))
            return

        body_text = self._link_body_text(link)
        href = _attr(_child(link, StorageTag.URL), StorageAttr.VALUE)
        if href:
            anchor = self.soup.new_tag("a", href=href)
            anchor.string = body_text or href
            link.replace_with(anchor)
            return

        # Without link text Confluence shows the target's name, so do the same.
        target = (
            body_text
            or _attr(
                _child(link, StorageTag.PAGE) or _child(link, StorageTag.BLOG_POST),
                StorageAttr.CONTENT_TITLE,
            )
            or _attr(_child(link, StorageTag.ATTACHMENT), StorageAttr.FILENAME)
            or _attr(_child(link, StorageTag.SPACE), StorageAttr.SPACE_KEY)
            or _attr(link, StorageAttr.ANCHOR)
        )
        self._replace_with_text(link, target)

    def _link_body_text(self, link: Tag) -> str:
        plain = _child(link, StorageTag.PLAIN_TEXT_LINK_BODY)
        if plain is not None:
            return plain.get_text().strip()
        rich = _child(link, StorageTag.LINK_BODY)
        if rich is not None:
            return rich.get_text(" ", strip=True)
        return ""

    def _mention_text(self, user: Tag) -> str:
        ref = UserRef(
            account_id=_attr(user, StorageAttr.ACCOUNT_ID) or None,
            userkey=_attr(user, StorageAttr.USERKEY) or None,
            username=_attr(user, StorageAttr.USERNAME) or None,
        )
        name = None
        if self.user_resolver is not None and not ref.is_empty():
            name = self.user_resolver(ref)
        return f"@{name}" if name else UNRESOLVED_USER_TEXT

    def _rewrite_image(self, image: Tag) -> None:
        label = (
            _attr(image, StorageAttr.ALT)
            or _attr(image, StorageAttr.TITLE)
            or _attr(_child(image, StorageTag.ATTACHMENT), StorageAttr.FILENAME)
        )
        src = _attr(_child(image, StorageTag.URL), StorageAttr.VALUE)
        if src:
            image.replace_with(self.soup.new_tag("img", src=src, alt=label))
            return
        self._replace_with_text(image, f"[Image: {label}]" if label else "")

    def _emoticon_text(self, emoticon: Tag) -> str:
        fallback = _attr(emoticon, StorageAttr.EMOJI_FALLBACK)
        if fallback:
            return fallback
        name = _attr(emoticon, StorageAttr.NAME)
        return f":{name}:" if name else ""
