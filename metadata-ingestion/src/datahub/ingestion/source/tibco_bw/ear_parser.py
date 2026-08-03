import glob
import io
import logging
import zipfile
from typing import Dict, Iterable, List, Optional, Tuple
from xml.etree.ElementTree import (  # nosec B405 - types only; parsing goes through defusedxml
    Element,
    ParseError,
)

import defusedxml.ElementTree as DET
from defusedxml.common import DefusedXmlException

from datahub.ingestion.source.tibco_bw.constants import (
    DESTINATION_ATTRIBUTE_NAMES,
    DESTINATION_TYPE_ATTRIBUTE_NAMES,
    DESTINATION_TYPE_TOPIC,
    EAR_NESTED_ARCHIVE_SUFFIXES,
    EAR_PROCESS_SUFFIX,
    EAR_SCHEMA_SUFFIX,
    EAR_SUBSTVAR_SUFFIX,
    FIELD_PATH_DELIMITER,
    JMS_ACTIVITY_TYPE_PREFIX,
    JMS_CONSUME_ACTIVITY_TYPES,
    JMS_PUBLISH_ACTIVITY_TYPES,
    MAX_SCHEMA_DEPTH,
    MESSAGE_ELEMENT_ATTRIBUTE_NAMES,
    PROPERTY_REFERENCE_PATTERN,
    XSD_NAMESPACE,
)
from datahub.ingestion.source.tibco_bw.models import (
    JmsDestinationType,
    JmsMessageField,
    JmsMessageSchema,
)
from datahub.ingestion.source.tibco_bw.report import TibcoBwSourceReport

logger: logging.Logger = logging.getLogger(__name__)

_XSD_ELEMENT = f"{{{XSD_NAMESPACE}}}element"
_XSD_COMPLEX_TYPE = f"{{{XSD_NAMESPACE}}}complexType"
_XSD_SIMPLE_TYPE = f"{{{XSD_NAMESPACE}}}simpleType"
_XSD_ATTRIBUTE = f"{{{XSD_NAMESPACE}}}attribute"
# A complex type's children hang off one of these; which one does not change the
# flattened result, only the cardinality the XSD intended.
_XSD_PARTICLES = tuple(
    f"{{{XSD_NAMESPACE}}}{name}" for name in ("sequence", "all", "choice")
)

_DEFAULT_XSD_TYPE = "string"
_UNBOUNDED = "unbounded"


class _Archive:
    """The parts of one EAR we can read, flattened across its nested bundles.

    An EAR is a zip of module JARs, so the processes and the schemas they
    reference are usually one level down. Both levels are flattened into a
    single name-to-bytes map because a schema import resolves by namespace and
    file name, not by which bundle it happened to be packaged in.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.entries: Dict[str, bytes] = {}

    def with_suffix(self, suffix: str) -> List[Tuple[str, bytes]]:
        return [
            (path, content)
            for path, content in sorted(self.entries.items())
            if path.endswith(suffix)
        ]


def parse_archives(
    paths: List[str], report: TibcoBwSourceReport
) -> List[JmsMessageSchema]:
    """Read the message schemas declared by the JMS activities in each archive.

    Every step is allowed to fail on its own: a process that cannot be parsed,
    an activity whose destination is an unresolvable property reference, or an
    element with no matching XSD is reported and skipped. One malformed process
    in a large estate must not cost the whole run.
    """
    schemas: List[JmsMessageSchema] = []
    for path in _expand(paths, report):
        archive = _read_archive(path, report)
        if archive is None:
            continue
        report.archives_read += 1
        properties = _module_properties(archive)
        elements = _schema_elements(archive, report)
        for process_path, content in archive.with_suffix(EAR_PROCESS_SUFFIX):
            schemas.extend(
                _parse_process(
                    archive, process_path, content, properties, elements, report
                )
            )
    return schemas


def _expand(paths: List[str], report: TibcoBwSourceReport) -> List[str]:
    expanded: List[str] = []
    for pattern in paths:
        matches = sorted(glob.glob(pattern))
        if not matches:
            report.warning(
                title="No application archive matched",
                message="The configured path matched no file, so any schema it "
                "would have declared is missing from this run.",
                context=pattern,
            )
        expanded.extend(matches)
    return expanded


def _read_archive(path: str, report: TibcoBwSourceReport) -> Optional[_Archive]:
    archive = _Archive(name=path)
    try:
        with zipfile.ZipFile(path) as outer:
            _collect(outer, archive, nested=True)
    except (zipfile.BadZipFile, OSError) as exc:
        report.warning(
            title="Application archive could not be read",
            message="The file is not a readable zip archive.",
            context=path,
            exc=exc,
        )
        return None
    return archive


def _collect(source: zipfile.ZipFile, archive: _Archive, nested: bool) -> None:
    for info in source.infolist():
        if info.is_dir():
            continue
        name = info.filename
        if nested and name.endswith(EAR_NESTED_ARCHIVE_SUFFIXES):
            try:
                with source.open(info) as raw:
                    with zipfile.ZipFile(io.BytesIO(raw.read())) as inner:
                        # One level only: BW nests module bundles inside the EAR,
                        # not bundles inside bundles.
                        _collect(inner, archive, nested=False)
            except (zipfile.BadZipFile, OSError) as exc:
                logger.debug(f"Skipping unreadable nested bundle {name}: {exc}")
            continue
        if name.endswith((EAR_PROCESS_SUFFIX, EAR_SCHEMA_SUFFIX, EAR_SUBSTVAR_SUFFIX)):
            try:
                archive.entries[name] = source.read(info)
            except (zipfile.BadZipFile, OSError) as exc:
                logger.debug(f"Skipping unreadable entry {name}: {exc}")


def _parse(content: bytes) -> Optional[Element]:
    try:
        return DET.fromstring(content.decode("utf-8", errors="replace"))
    except (ParseError, DefusedXmlException, ValueError) as exc:
        logger.debug(f"XML parse failed: {exc}")
        return None


def _module_properties(archive: _Archive) -> Dict[str, str]:
    """Values for the `%%name%%` references an activity's destination may use.

    BW resolves a property through process, then module, then application scope.
    The archive carries the deploy-time values in `.substvar` files, and later
    files win so that an application-level override beats the module default -
    which is the order BW itself resolves in.
    """
    properties: Dict[str, str] = {}
    for _, content in archive.with_suffix(EAR_SUBSTVAR_SUFFIX):
        root = _parse(content)
        if root is None:
            continue
        for element in root.iter():
            name = _child_text(element, "name")
            value = _child_text(element, "value")
            if name:
                properties[name] = value or ""
    return properties


def _child_text(element: Element, local_name: str) -> Optional[str]:
    for child in element:
        if _local(child.tag) == local_name:
            return (child.text or "").strip()
    return None


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _schema_elements(
    archive: _Archive, report: TibcoBwSourceReport
) -> Dict[str, Tuple[Element, Element, str]]:
    """Top-level XSD element declarations, keyed by local name.

    Keyed by local name rather than by qualified name because an activity
    references its element with a namespace prefix bound in the process, and
    those prefixes are not stable across modules. A name declared twice with
    different shapes is reported rather than silently resolved.
    """
    elements: Dict[str, Tuple[Element, Element, str]] = {}
    for path, content in archive.with_suffix(EAR_SCHEMA_SUFFIX):
        root = _parse(content)
        if root is None:
            report.warning(
                title="Schema in an application archive could not be parsed",
                message="Any message element it declares cannot be resolved.",
                context=f"{archive.name}:{path}",
            )
            continue
        raw = content.decode("utf-8", errors="replace")
        for element in root:
            if element.tag != _XSD_ELEMENT:
                continue
            name = element.get("name")
            if not name:
                continue
            if name in elements:
                report.report_duplicate_schema_element(name)
                continue
            elements[name] = (element, root, raw)
    return elements


def _parse_process(
    archive: _Archive,
    process_path: str,
    content: bytes,
    properties: Dict[str, str],
    elements: Dict[str, Tuple[Element, Element, str]],
    report: TibcoBwSourceReport,
) -> List[JmsMessageSchema]:
    root = _parse(content)
    if root is None:
        report.warning(
            title="Process in an application archive could not be parsed",
            message="Its JMS activities contribute no message schemas.",
            context=f"{archive.name}:{process_path}",
        )
        return []
    report.processes_read += 1

    schemas: List[JmsMessageSchema] = []
    for activity, activity_type in _jms_activities(root):
        report.jms_activities_found += 1
        schema = _activity_schema(
            archive,
            process_path,
            activity,
            activity_type,
            properties,
            elements,
            report,
        )
        if schema is not None:
            schemas.append(schema)
    return schemas


def _jms_activities(root: Element) -> Iterable[Tuple[Element, str]]:
    # Found by activity type id rather than element path: BW5, BW6 and BWCE wrap
    # the activity in different extension elements but agree on the type id.
    for element in root.iter():
        for value in element.attrib.values():
            if value.startswith(JMS_ACTIVITY_TYPE_PREFIX):
                yield element, value
                break


def _activity_schema(
    archive: _Archive,
    process_path: str,
    activity: Element,
    activity_type: str,
    properties: Dict[str, str],
    elements: Dict[str, Tuple[Element, Element, str]],
    report: TibcoBwSourceReport,
) -> Optional[JmsMessageSchema]:
    publishes = activity_type in JMS_PUBLISH_ACTIVITY_TYPES
    if not publishes and activity_type not in JMS_CONSUME_ACTIVITY_TYPES:
        # A JMS activity that carries no message body of its own, e.g. a reply
        # that reuses the request's schema.
        return None

    raw_destination = _find_attribute(activity, DESTINATION_ATTRIBUTE_NAMES)
    if raw_destination is None:
        report.report_activity_without_destination(f"{archive.name}:{process_path}")
        return None
    destination = _resolve_properties(raw_destination, properties)
    if destination is None:
        report.report_unresolved_destination(
            f"{archive.name}:{process_path}: {raw_destination}"
        )
        return None

    element_name = _find_attribute(activity, MESSAGE_ELEMENT_ATTRIBUTE_NAMES)
    if element_name is None:
        report.report_activity_without_element(destination)
        return None
    local_name = element_name.rsplit(":", 1)[-1]
    resolved = elements.get(local_name)
    if resolved is None:
        report.report_unresolved_element(f"{destination}: {element_name}")
        return None

    declaration, schema_root, raw_schema = resolved
    fields = _flatten(declaration, schema_root, prefix="", depth=0)
    if not fields:
        report.report_element_without_fields(f"{destination}: {local_name}")
        return None

    return JmsMessageSchema(
        destination_name=destination,
        destination_type=_destination_type(activity),
        fields=fields,
        raw_schema=raw_schema,
        declared_by=process_path,
        archive=archive.name,
        element_name=local_name,
        publishes=publishes,
    )


def _find_attribute(activity: Element, names: Tuple[str, ...]) -> Optional[str]:
    # Checked on the activity and then on its subtree, because the configuration
    # sits on a child element in some BW versions and on the activity itself in
    # others. Attribute names are matched without their namespace.
    for candidates in (activity.attrib, *(child.attrib for child in activity.iter())):
        for name in names:
            for key, value in candidates.items():
                if _local(key) == name and value.strip():
                    return value.strip()
    return None


def _resolve_properties(value: str, properties: Dict[str, str]) -> Optional[str]:
    # A destination built by string concatenation at runtime cannot be resolved
    # statically, so an unresolved reference returns None rather than a name with
    # a %%placeholder%% left in it - that would attach the schema to a topic that
    # does not exist.
    resolved = value
    for match in PROPERTY_REFERENCE_PATTERN.finditer(value):
        replacement = properties.get(match.group(1))
        if replacement is None:
            return None
        resolved = resolved.replace(match.group(0), replacement)
    return resolved or None


def _destination_type(activity: Element) -> JmsDestinationType:
    declared = _find_attribute(activity, DESTINATION_TYPE_ATTRIBUTE_NAMES)
    if declared and DESTINATION_TYPE_TOPIC in declared.casefold():
        return JmsDestinationType.TOPIC
    # Queue is the BW default for a JMS activity with no explicit kind.
    return JmsDestinationType.QUEUE


def _flatten(
    declaration: Element, schema_root: Element, prefix: str, depth: int
) -> List[JmsMessageField]:
    """Turn an XSD element declaration into dot-delimited field paths."""
    if depth >= MAX_SCHEMA_DEPTH:
        # A self-referencing type (an order line holding order lines) would
        # otherwise recurse until the stack gives out.
        return []
    complex_type = _complex_type(declaration, schema_root)
    if complex_type is None:
        return [
            JmsMessageField(
                path=prefix or declaration.get("name", ""),
                xsd_type=_type_name(declaration),
                nullable=_nullable(declaration),
            )
        ]

    fields: List[JmsMessageField] = []
    for child in _particle_children(complex_type):
        name = child.get("name")
        if not name:
            continue
        path = f"{prefix}{FIELD_PATH_DELIMITER}{name}" if prefix else name
        fields.extend(_flatten(child, schema_root, path, depth + 1))
    for attribute in complex_type:
        if attribute.tag != _XSD_ATTRIBUTE:
            continue
        name = attribute.get("name")
        if name:
            path = f"{prefix}{FIELD_PATH_DELIMITER}{name}" if prefix else name
            fields.append(
                JmsMessageField(
                    path=path,
                    xsd_type=_type_name(attribute),
                    nullable=attribute.get("use") != "required",
                )
            )
    return fields


def _complex_type(declaration: Element, schema_root: Element) -> Optional[Element]:
    for child in declaration:
        if child.tag == _XSD_COMPLEX_TYPE:
            return child
        if child.tag == _XSD_SIMPLE_TYPE:
            return None
    named = declaration.get("type")
    if not named:
        return None
    local_name = named.rsplit(":", 1)[-1]
    for candidate in schema_root:
        if candidate.tag == _XSD_COMPLEX_TYPE and candidate.get("name") == local_name:
            return candidate
    return None


def _particle_children(complex_type: Element) -> Iterable[Element]:
    for particle in complex_type:
        if particle.tag not in _XSD_PARTICLES:
            continue
        for child in particle:
            if child.tag == _XSD_ELEMENT:
                yield child
            elif child.tag in _XSD_PARTICLES:
                # A choice nested in a sequence: every branch is a field the
                # message may carry, so all of them are kept.
                for nested in child:
                    if nested.tag == _XSD_ELEMENT:
                        yield nested


def _type_name(declaration: Element) -> str:
    named = declaration.get("type")
    if named:
        return named.rsplit(":", 1)[-1]
    for child in declaration:
        if child.tag == _XSD_SIMPLE_TYPE:
            for restriction in child:
                base = restriction.get("base")
                if base:
                    return base.rsplit(":", 1)[-1]
    return _DEFAULT_XSD_TYPE


def _nullable(declaration: Element) -> bool:
    if declaration.get("nillable") == "true":
        return True
    if declaration.get("maxOccurs") == _UNBOUNDED:
        return True
    return declaration.get("minOccurs", "1") == "0"
