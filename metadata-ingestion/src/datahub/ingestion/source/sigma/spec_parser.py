from dataclasses import dataclass, field
from typing import (
    AbstractSet,
    Annotated,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from datahub.ingestion.source.sigma.formula_parser import extract_bracket_refs

# The `schemaVersion` this parser was written against. A consumer should
# distrust a read whose `schema_version` differs.
SUPPORTED_SCHEMA_VERSION = 1
# A control's `source` binds its value to a column; it carries no lineage. Its
# `controlId` is the name a formula uses to read the control as a parameter.
_CONTROL_ELEMENT_KIND = "control"
# Sigma's schema requires `source` on a table element; other elements, such as
# a text box or a control's display, may have none.
_TABLE_ELEMENT_KIND = "table"
_SOURCE = "source"
_DATA_MODEL_ID = "dataModelId"
_ELEMENT_ID = "elementId"
_JOIN_KIND = "join"
_UNION_KIND = "union"
# Every source kind in Sigma's create-spec schema and published examples is one
# of these. Single-source kinds are ones /columns formulas already reach; a
# `data-model` source is an element of another Data Model. A transpose is valid
# Sigma this parser does not map: its upstream columns appear only in
# `columnsToMerge`. Any other kind -- a renamed "join", say -- is drift.
_SINGLE_SOURCE_KINDS = frozenset(
    {"warehouse-table", "table", "sql", "csv-table", "data-model"}
)
_UNMAPPED_KINDS = frozenset({"transpose"})
_INNER_JOIN_TYPE = "inner"
# The join types Sigma's write API accepts. `lookup` is normalised to
# `left-outer` on store, and is kept in case a spec carries it anyway. A stored
# spec always carries `joinType` -- Sigma's published join example has it even
# for an inner join -- so a missing or unknown one is reported, not scored.
_OUTER_JOIN_TYPES = frozenset({"left-outer", "right-outer", "full-outer", "lookup"})
_KNOWN_JOIN_TYPES = _OUTER_JOIN_TYPES | {_INNER_JOIN_TYPE}
# Only equality says two columns hold the same value; the others make a column a
# join participant without making it equal. These are the operators Sigma's
# data model write API accepts and stores verbatim. The UI's null-safe `<=>` and
# `<!=>` are stored as `is-not-distinct-from` and `is-distinct-from`; `<=>`
# itself is rejected. An operator outside the set is reported, so a wrong entry
# fails closed. A missing `op` is equality: Sigma's published join example
# omits it.
_EQUALITY_OP = "="
_EQUALITY_OPS = frozenset({_EQUALITY_OP, "is-not-distinct-from"})
_KNOWN_OPS = _EQUALITY_OPS | {
    "!=",
    "is-distinct-from",
    "<",
    "<=",
    ">",
    ">=",
    "within",
    "intersects",
}


@dataclass(frozen=True)
class SpecColumnRef:
    """A column named by a join side or a union branch.

    ``element_id`` is None for a warehouse table, whose columns appear nowhere
    in the spec; ``connection_id`` and ``path`` then identify the table for the
    caller to map. ``column`` is the display name the formula references;
    ``expression`` is the raw formula.
    """

    element_id: Optional[str]
    column: str
    expression: str = ""
    # Set for an element in another Data Model. Element ids are not unique
    # across models, so the column cannot be resolved without it.
    data_model_id: Optional[str] = None
    connection_id: Optional[str] = None
    path: Tuple[str, ...] = ()
    # A union branch's position in ``sources``; None for a join side.
    source_index: Optional[int] = None


@dataclass(frozen=True)
class UnionOutputColumn:
    """A ``union`` element's output column and the branch columns it merges.

    A union output's ``/columns`` formula names at most one branch; the spec
    names all of them, including warehouse-table branches.
    """

    union_element_id: str
    output_column: str
    # In ``sources`` order. A branch whose formula names several columns, such
    # as ``Concat([First], " ", [Last])``, contributes one ref per column, all
    # with the same ``source_index``.
    branches: Tuple[SpecColumnRef, ...]


@dataclass(frozen=True)
class JoinPredicate:
    """A key from a join's ON clause.

    ``left.column`` and ``right.column`` are the columns the join matches on.
    A side may transform its column (``[A] + 1``, ``Left([A], 3)``), so this is
    key participation, not value equality; ``expression`` keeps each formula.
    Under an outer join the key holds only on matched rows, so consumers score
    those edges lower. Either side may be a warehouse table (``element_id`` is
    None, ``connection_id`` and ``path`` set); whether to emit a key edge to
    one is the consumer's call. Identical predicates in different joins are not
    deduplicated.

    Each side's column belongs to that join entry's own ``left`` / ``right``
    descriptor, including in a chained join. Sigma's schema requires each
    entry's ``left`` to be the primary source or an earlier entry's ``right``,
    and its write API resolves each side's formula against that descriptor --
    naming a column the descriptor does not own is rejected as "Column
    reference not found".
    """

    join_element_id: str
    left: SpecColumnRef
    right: SpecColumnRef
    join_type: str

    @property
    def is_outer(self) -> bool:
        return self.join_type in _OUTER_JOIN_TYPES


@dataclass
class DataModelSpecIndex:
    """Join predicates and union columns read from a Data Model's /spec.

    An element whose shape does not match what this parser expects is reported,
    even when part of it was read: partial drift is the likeliest kind, and a
    consumer should not mistake it for a complete read. ``drift_detected`` is
    that judgement in one place.

    ``relationships[]`` is deliberately not read as lineage. A relationship is
    a declared join that moves no data until a formula uses it as
    ``[Element/Relationship/Column]``; resolving such a ref needs the
    relationship's target, which is for the resolver of those refs to index.
    """

    pairs: List[JoinPredicate] = field(default_factory=list)
    unions: List[UnionOutputColumn] = field(default_factory=list)
    unreadable_join_element_ids: List[str] = field(default_factory=list)
    unreadable_union_element_ids: List[str] = field(default_factory=list)
    # Elements whose own shape is not recognised, so a renamed key shows up
    # here instead of as a clean, empty index: by kind for an element with an
    # id and a kind this parser does not know, and as a bare count for one that
    # cannot be named (a non-object source, a missing or blank id or kind).
    unrecognised_kind_element_ids: Dict[str, List[str]] = field(default_factory=dict)
    unrecognised_element_count: int = 0
    # False when `pages`, or a page's `elements`, is not a list: a renamed key
    # there would otherwise read as an empty model, which is a real thing.
    structure_readable: bool = True
    # Every element seen, and those with a source.
    element_count: int = 0
    sourced_element_count: int = 0
    # Valid Sigma this parser reads but does not map, so the lineage left
    # behind is visible without being mistaken for drift: elements of a known
    # but unmapped kind, keyed by kind, and join or union elements with a
    # multi-segment ref (`[Element/Col]`, `[Element/Relationship/Col]`), which
    # needs the element or relationship it names.
    unmapped_element_ids: Dict[str, List[str]] = field(default_factory=dict)
    multi_segment_ref_element_ids: List[str] = field(default_factory=list)
    # The document's `schemaVersion`; see SUPPORTED_SCHEMA_VERSION.
    schema_version: Optional[int] = None

    @property
    def is_supported_schema(self) -> bool:
        return self.schema_version == SUPPORTED_SCHEMA_VERSION

    @property
    def drift_detected(self) -> bool:
        """Whether the read should not be trusted as complete.

        Unmapped kinds and multi-segment refs are valid Sigma, not drift, and an
        empty model is a real one; a renamed `pages`, `elements` or `source` key
        shows up through `structure_readable` and the unrecognised counts.
        """
        return bool(
            self.unreadable_join_element_ids
            or self.unreadable_union_element_ids
            or self.unrecognised_kind_element_ids
            or self.unrecognised_element_count
            or not self.structure_readable
            or not self.is_supported_schema
        )


# --- Shape: what a /spec document must look like -----------------------------
#
# Unknown keys are ignored: Sigma adds fields (`groupingId`, `name`) that carry
# no lineage. Lists whose entries are judged one by one are typed `List[Any]`
# and validated entry by entry, so one broken entry does not hide the others.


def _not_blank(value: str) -> str:
    if not value.strip():
        raise ValueError("blank")
    return value


def _normalised(value: str) -> str:
    return value.strip().lower()


def _non_blank_or_none(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and value.strip() else None


def _normalised_or_none(value: Any) -> Optional[str]:
    return _normalised(value) if isinstance(value, str) else None


_NonBlank = Annotated[StrictStr, AfterValidator(_not_blank)]
_Normalised = Annotated[StrictStr, AfterValidator(_normalised)]
# An element's own fields are read one by one: a bad id must not lose its kind.
_NonBlankOrNone = Annotated[Optional[str], BeforeValidator(_non_blank_or_none)]
_NormalisedOrNone = Annotated[Optional[str], BeforeValidator(_normalised_or_none)]


class _Shape(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)


class _LocalElement(_Shape):
    """An element of this Data Model."""

    kind: Literal["table"]
    elementId: _NonBlank

    @model_validator(mode="before")
    @classmethod
    def _no_model_id(cls, values: Any) -> Any:
        # Sigma stores another model's element as `data-model`, and strips a
        # `dataModelId` posted on a `table` side, so carrying one is drift.
        if isinstance(values, dict) and _DATA_MODEL_ID in values:
            raise ValueError("a table side names no Data Model")
        return values


class _OtherModelElement(_Shape):
    """An element of a Data Model, possibly this one."""

    kind: Literal["data-model"]
    dataModelId: _NonBlank
    elementId: _NonBlank


class _WarehouseTable(_Shape):
    # The consumer builds the table's URN from both.
    kind: Literal["warehouse-table"]
    connectionId: _NonBlank
    path: Annotated[
        List[Annotated[StrictStr, Field(min_length=1)]], Field(min_length=1)
    ]

    @model_validator(mode="before")
    @classmethod
    def _no_element(cls, values: Any) -> Any:
        if isinstance(values, dict) and _ELEMENT_ID in values:
            raise ValueError("a warehouse table names no element")
        return values


_Descriptor = Annotated[
    Union[_LocalElement, _OtherModelElement, _WarehouseTable],
    Field(discriminator="kind"),
]


class _JoinEntry(_Shape):
    """One ON-clause predicate. A side is a required formula."""

    left: _NonBlank
    right: _NonBlank
    # Sigma's published join example omits it.
    op: _Normalised = _EQUALITY_OP

    @field_validator("op", mode="before")
    @classmethod
    def _missing_op_is_equality(cls, op: Any) -> Any:
        return _EQUALITY_OP if op is None else op

    @field_validator("op")
    @classmethod
    def _known_op(cls, op: str) -> str:
        if op not in _KNOWN_OPS:
            raise ValueError("unknown operator")
        return op


class _Join(_Shape):
    joinType: _Normalised
    left: _Descriptor
    right: _Descriptor
    # A join with no predicates says nothing about what it matches on.
    columns: Annotated[List[Any], Field(min_length=1)]

    @field_validator("joinType")
    @classmethod
    def _known_join_type(cls, join_type: str) -> str:
        if join_type not in _KNOWN_JOIN_TYPES:
            raise ValueError("unknown join type")
        return join_type


class _JoinSource(_Shape):
    joins: List[Any]


class _UnionMatch(_Shape):
    outputColumnName: Annotated[StrictStr, Field(min_length=1)]
    # `sourceColumns[i]` is the formula branch `sources[i]` contributes.
    sourceColumns: List[Any]


class _UnionSource(_Shape):
    sources: List[Any]
    matches: List[Any]


class _Element(_Shape):
    id: _NonBlankOrNone = None
    kind: _NormalisedOrNone = None
    controlId: _NonBlankOrNone = None


class _SourceKind(_Shape):
    kind: _Normalised


_T = TypeVar("_T")
_S = TypeVar("_S", bound=_Shape)

_DESCRIPTOR: TypeAdapter[Union[_LocalElement, _OtherModelElement, _WarehouseTable]] = (
    TypeAdapter(_Descriptor)
)
# An empty slot is a branch contributing nothing.
_UNION_SLOT: TypeAdapter[Optional[StrictStr]] = TypeAdapter(Optional[StrictStr])
_SCHEMA_VERSION: TypeAdapter[StrictInt] = TypeAdapter(StrictInt)
_NON_BLANK: TypeAdapter[str] = TypeAdapter(_NonBlank)


def _valid(adapter: TypeAdapter[_T], value: Any) -> Optional[_T]:
    try:
        return adapter.validate_python(value)
    except ValidationError:
        return None


def _model(cls: Type[_S], value: Any) -> Optional[_S]:
    try:
        return cls.model_validate(value)
    except ValidationError:
        return None


# --- Meaning: this parser's rules, over checked shapes -------------------------


@dataclass(frozen=True)
class _Owner:
    """Whose column a join side or union branch names."""

    element_id: Optional[str]
    data_model_id: Optional[str] = None
    connection_id: Optional[str] = None
    path: Tuple[str, ...] = ()


@dataclass
class _JoinRead:
    predicates: List[JoinPredicate]
    readable: bool
    multi_segment_refs: bool = False


@dataclass
class _UnionRead:
    columns: List[UnionOutputColumn]
    readable: bool
    multi_segment_refs: bool = False


@dataclass
class _Elements:
    elements: List[Dict[str, Any]]
    readable: bool


def _iter_spec_elements(spec: Dict[str, Any]) -> _Elements:
    pages = spec.get("pages")
    if not isinstance(pages, list):
        return _Elements(elements=[], readable=False)
    elements: List[Dict[str, Any]] = []
    readable = True
    for page in pages:
        page_elements = page.get("elements") if isinstance(page, dict) else None
        if not isinstance(page_elements, list):
            readable = False
            continue
        for element in page_elements:
            if isinstance(element, dict):
                elements.append(element)
            else:
                readable = False
    return _Elements(elements=elements, readable=readable)


def _owner(
    descriptor: Union[_LocalElement, _OtherModelElement, _WarehouseTable],
    own_data_model_id: Optional[str],
) -> _Owner:
    if isinstance(descriptor, _WarehouseTable):
        return _Owner(
            element_id=None,
            connection_id=descriptor.connectionId,
            path=tuple(descriptor.path),
        )
    if isinstance(descriptor, _OtherModelElement):
        # This model's own id names a local element.
        if descriptor.dataModelId != own_data_model_id:
            return _Owner(
                element_id=descriptor.elementId,
                data_model_id=descriptor.dataModelId,
            )
    return _Owner(element_id=descriptor.elementId)


def _join_side_columns(
    expression: str, parameters: AbstractSet[str]
) -> Optional[Set[str]]:
    """The distinct columns a join side's formula references, or None for a
    multi-segment ref, which names a relationship or another element.

    Sides are formulas, not identifiers: ``[Col A]`` or ``Coalesce([Col A],
    -2)``. Names are compared exactly, so ``[Key]`` and ``[KEY]`` are two
    columns; Sigma's case rule is unverified, and two is the reading that
    claims less. ``parameters`` are the model's control ids: a ref to one is a
    constant, and any other ref is a column, whatever its name looks like.
    """
    refs = extract_bracket_refs(expression)
    if any(ref.column is not None for ref in refs):
        return None
    return {ref.source for ref in refs if ref.source not in parameters}


def _one_column(names: Optional[Set[str]]) -> Optional[str]:
    """A key is one distinct column, even inside a function or repeated."""
    return next(iter(names)) if names is not None and len(names) == 1 else None


def _column_ref(
    owner: _Owner,
    formula: str,
    column: str,
    *,
    source_index: Optional[int] = None,
) -> SpecColumnRef:
    return SpecColumnRef(
        element_id=owner.element_id,
        column=column,
        expression=formula,
        data_model_id=owner.data_model_id,
        connection_id=owner.connection_id,
        path=owner.path,
        source_index=source_index,
    )


def _read_join(
    raw: Any,
    *,
    join_element_id: str,
    parameters: AbstractSet[str],
    own_data_model_id: Optional[str],
) -> _JoinRead:
    """One entry of a join element's ``joins``.

    Readability is decided on SHAPE, before any formula is resolved: a literal
    or composite side is a well-formed predicate that is simply not a key.
    """
    join = _model(_Join, raw)
    if join is None:
        return _JoinRead(predicates=[], readable=False)
    left_owner = _owner(join.left, own_data_model_id)
    right_owner = _owner(join.right, own_data_model_id)

    predicates: List[JoinPredicate] = []
    readable = True
    multi_segment_refs = False
    for raw_entry in join.columns:
        entry = _model(_JoinEntry, raw_entry)
        if entry is None:
            readable = False
            continue
        if entry.op not in _EQUALITY_OPS:
            continue
        # A literal or composite side is a well-formed predicate, not a key; a
        # multi-segment one is also recorded, since it may be a key we cannot map.
        left_names = _join_side_columns(entry.left, parameters)
        right_names = _join_side_columns(entry.right, parameters)
        if left_names is None or right_names is None:
            multi_segment_refs = True
        left_column = _one_column(left_names)
        right_column = _one_column(right_names)
        if left_column is None or right_column is None:
            continue
        # A self-join on one column is not an edge.
        if (left_owner, left_column) == (right_owner, right_column):
            continue
        predicates.append(
            JoinPredicate(
                join_element_id=join_element_id,
                left=_column_ref(left_owner, entry.left, left_column),
                right=_column_ref(right_owner, entry.right, right_column),
                join_type=join.joinType,
            )
        )
    return _JoinRead(
        predicates=predicates,
        readable=readable,
        multi_segment_refs=multi_segment_refs,
    )


def _read_union(
    raw: Dict[str, Any],
    *,
    union_element_id: str,
    parameters: AbstractSet[str],
    own_data_model_id: Optional[str],
) -> _UnionRead:
    """A ``union`` element's source.

    Shape: ``{"kind": "union", "sources": [<descriptor>, ...],
    "matches": [{"outputColumnName": ..., "sourceColumns": [...]}]}``, where
    ``sourceColumns[i]`` is the formula branch ``sources[i]`` contributes and
    an empty slot is a branch contributing nothing. A branch may be a
    warehouse table.
    """
    union = _model(_UnionSource, raw)
    if union is None:
        return _UnionRead(columns=[], readable=False)
    descriptors = [_valid(_DESCRIPTOR, branch) for branch in union.sources]
    owners = [
        _owner(descriptor, own_data_model_id) if descriptor is not None else None
        for descriptor in descriptors
    ]
    # A union with sources but no output columns says nothing about what it
    # produces, like a join with no predicates.
    readable = all(owner is not None for owner in owners) and bool(
        union.matches or not union.sources
    )
    multi_segment_refs = False

    columns: List[UnionOutputColumn] = []
    for raw_match in union.matches:
        match = _model(_UnionMatch, raw_match)
        if match is None:
            readable = False
            continue
        branches: List[SpecColumnRef] = []
        for position, raw_formula in enumerate(match.sourceColumns):
            # Positional alignment is inferred, not stated, so a slot with no
            # branch is reported rather than paired with another branch.
            if position >= len(owners):
                readable = False
                continue
            if raw_formula is None or raw_formula == "":
                continue
            formula = _valid(_UNION_SLOT, raw_formula)
            if formula is None:
                readable = False
                continue
            # A branch is a data flow, not a key: every column it names feeds
            # the output, and a parameter is a constant it can contribute. No
            # column at all is a branch contributing a constant.
            refs = extract_bracket_refs(formula)
            if any(r.column is not None for r in refs):
                multi_segment_refs = True
            owner = owners[position]
            if owner is None:
                continue
            names = {
                r.source
                for r in refs
                if r.column is None and r.source not in parameters
            }
            # Sorted, not formula order: set order would depend on the hash seed.
            for name in sorted(names):
                branches.append(
                    _column_ref(owner, formula, name, source_index=position)
                )
        if branches:
            columns.append(
                UnionOutputColumn(
                    union_element_id=union_element_id,
                    output_column=match.outputColumnName,
                    branches=tuple(branches),
                )
            )
    return _UnionRead(
        columns=columns, readable=readable, multi_segment_refs=multi_segment_refs
    )


def _read_join_element(
    raw: Dict[str, Any],
    *,
    element_id: str,
    parameters: AbstractSet[str],
    own_data_model_id: Optional[str],
    index: DataModelSpecIndex,
) -> None:
    source = _model(_JoinSource, raw)
    if source is None:
        index.unreadable_join_element_ids.append(element_id)
        return
    # Judged per join: one readable join must not hide a broken one.
    readable = True
    multi_segment_refs = False
    for join in source.joins:
        read = _read_join(
            join,
            join_element_id=element_id,
            parameters=parameters,
            own_data_model_id=own_data_model_id,
        )
        index.pairs.extend(read.predicates)
        readable = readable and read.readable
        multi_segment_refs = multi_segment_refs or read.multi_segment_refs
    if multi_segment_refs:
        index.multi_segment_ref_element_ids.append(element_id)
    if not readable:
        index.unreadable_join_element_ids.append(element_id)


def parse_data_model_spec(spec: Optional[Dict[str, Any]]) -> DataModelSpecIndex:
    """Join-key equivalences and union branches from a Data Model /spec.

    A join's output column names only one side in its formula, so the other
    side's key column is reachable only from the ON clause::

        source = {"kind": "join",
                  "primarySource": {...},
                  "joins": [{"joinType": ..., "left": {...}, "right": {...},
                             "columns": [{"left": ..., "right": ..., "op": ...}]}]}

    For the union shape see :func:`_read_union`.
    """
    index = DataModelSpecIndex()
    if not isinstance(spec, dict):
        return index
    index.schema_version = _valid(_SCHEMA_VERSION, spec.get("schemaVersion"))
    own_data_model_id = _valid(_NON_BLANK, spec.get(_DATA_MODEL_ID))
    walked = _iter_spec_elements(spec)
    index.structure_readable = walked.readable
    elements = [(raw, _Element.model_validate(raw)) for raw in walked.elements]
    parameters = frozenset(
        element.controlId
        for _, element in elements
        if element.kind == _CONTROL_ELEMENT_KIND and element.controlId
    )

    for raw, element in elements:
        index.element_count += 1
        # A control's source only binds its value to a column.
        if element.kind == _CONTROL_ELEMENT_KIND:
            continue
        if _SOURCE not in raw:
            # A text box has no source; a table element always does.
            if element.kind == _TABLE_ELEMENT_KIND:
                index.unrecognised_element_count += 1
            continue
        index.sourced_element_count += 1
        source = raw[_SOURCE]
        source_kind = _model(_SourceKind, source)
        if element.id is None or source_kind is None or not source_kind.kind:
            index.unrecognised_element_count += 1
            continue
        element_id = element.id
        kind = source_kind.kind
        if kind == _UNION_KIND:
            union = _read_union(
                source,
                union_element_id=element_id,
                parameters=parameters,
                own_data_model_id=own_data_model_id,
            )
            index.unions.extend(union.columns)
            if union.multi_segment_refs:
                index.multi_segment_ref_element_ids.append(element_id)
            if not union.readable:
                index.unreadable_union_element_ids.append(element_id)
        elif kind == _JOIN_KIND:
            _read_join_element(
                source,
                element_id=element_id,
                parameters=parameters,
                own_data_model_id=own_data_model_id,
                index=index,
            )
        elif kind in _UNMAPPED_KINDS:
            index.unmapped_element_ids.setdefault(kind, []).append(element_id)
        elif kind not in _SINGLE_SOURCE_KINDS:
            index.unrecognised_kind_element_ids.setdefault(kind, []).append(element_id)
    return index
