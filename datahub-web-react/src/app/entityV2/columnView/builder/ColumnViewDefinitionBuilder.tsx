import { ArrowDownOutlined, ArrowUpOutlined, SettingOutlined, VerticalAlignTopOutlined } from '@ant-design/icons';
import { useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { Button, Checkbox, Input, InputNumber, Popover, Select, Tooltip, Typography } from 'antd';
import React, { useMemo, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import LabelPicker from '@app/entityV2/columnView/builder/LabelPicker';
import StructuredPropertyPicker from '@app/entityV2/columnView/builder/StructuredPropertyPicker';
import {
    ATTRIBUTE_KINDS,
    COLUMN_KIND_SPECS,
    ColumnLike,
    LABEL_FILTER_FIELDS,
    MAX_GRAPH_COLUMNS,
    STRUCTURED_PROPERTY_FILTER_PREFIX,
    availableRelationshipKinds,
    columnHeader,
    columnHeaderHint,
    columnIdentity,
    customDisplayValue,
    displayFor,
    isGraphColumn,
    isLabelColumn,
    isSortableColumn,
    isStructuredPropertyColumn,
    structuredPropertyUrnOf,
    withDisplay,
} from '@app/entityV2/columnView/columnKinds';
import ColumnViewSortableList, { MoveDirection, moveItem } from '@app/entityV2/columnView/ColumnViewSortableList';
import '@app/entityV2/columnView/renderers/builtIn';
import { getRenderersFor } from '@app/entityV2/columnView/renderers/registry';
import { ColumnViewBuilderState, RENDERER_CUSTOM_KEY, WIDTH_PRESETS } from '@app/entityV2/columnView/types';
import { useIsNarrowViewport } from '@app/entityV2/columnView/useIsNarrowViewport';
import { useStructuredPropertyNames } from '@app/entityV2/columnView/useStructuredPropertyNames';
import DragHandle from '@app/homeV3/modules/assetCollection/dragAndDrop/DragHandle';
import { useAppConfig } from '@app/useAppConfig';

import {
    DataHubColumnViewColumnInput,
    DataHubColumnViewColumnType,
    DataHubColumnViewExpand,
    DataHubColumnViewLabelStyle,
    DataHubColumnViewOverflow,
    FilterOperator,
    LogicalOperator,
    SortOrder,
} from '@types';

const Panes = styled.div<{ $stacked?: boolean }>`
    display: grid;
    grid-template-columns: ${(p) => (p.$stacked ? '1fr' : '1fr 1fr')};
    gap: 16px;
`;

const Pane = styled.div`
    min-height: 240px;
    padding: 8px;
    border: 1px solid ${(p) => p.theme.colors.border};
    border-radius: 8px;
`;

const Row = styled.div<{ $transform?: string; $transition?: string; $isDragging?: boolean }>`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 8px;
    border-radius: 8px;
    background-color: ${(p) => p.theme.colors.bg};
    box-shadow: ${(p) => (p.$isDragging ? p.theme.colors.shadowSm : 'none')};
    transform: ${(p) => p.$transform};
    transition: ${(p) => p.$transition};
`;

const Section = styled.div`
    margin-top: 12px;
`;

const Inline = styled.div`
    display: flex;
    gap: 8px;
    margin-top: 4px;
`;

const Hint = styled(Typography.Text)`
    margin-left: 6px;
    font-size: 11px;
`;

const Settings = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    width: 260px;
`;

const AvailableRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

type Props = {
    state: ColumnViewBuilderState;
    updateState: (next: ColumnViewBuilderState) => void;
};

/**
 * Filter fields the client-side evaluator understands (filterSchemaRowsByView). Relationship
 * (GRAPH) kinds are paged previews, not row attributes, so they are never offered here.
 */
const FILTER_FIELDS = [
    'fieldPath',
    'nativeDataType',
    'nullable',
    'isPartOfKey',
    'isPartitioningKey',
    'length',
    'precision',
    'scale',
    'description',
    'tags',
    'glossaryTerms',
];
const OPERATORS: { value: string; labelKey: string; negated?: boolean; unary?: boolean; ordered?: boolean }[] = [
    { value: 'EQUAL', labelKey: 'columnViews.operator.equals' },
    { value: 'CONTAIN', labelKey: 'columnViews.operator.contains' },
    { value: 'START_WITH', labelKey: 'columnViews.operator.startsWith' },
    { value: 'END_WITH', labelKey: 'columnViews.operator.endsWith' },
    { value: 'GREATER_THAN', labelKey: 'columnViews.operator.greaterThan', ordered: true },
    { value: 'GREATER_THAN_OR_EQUAL_TO', labelKey: 'columnViews.operator.greaterThanOrEqual', ordered: true },
    { value: 'LESS_THAN', labelKey: 'columnViews.operator.lessThan', ordered: true },
    { value: 'LESS_THAN_OR_EQUAL_TO', labelKey: 'columnViews.operator.lessThanOrEqual', ordered: true },
    { value: 'EXISTS', labelKey: 'columnViews.operator.exists', unary: true },
    { value: 'NOT_EXISTS', labelKey: 'columnViews.operator.notExists', unary: true, negated: true },
];
const isLabelField = (field: string) => LABEL_FILTER_FIELDS.includes(field);

type WidthPreset = keyof typeof WIDTH_PRESETS | 'custom';

/** Gear popover: Format (registry), Width, and for GRAPH kinds label style / max items / overflow / expand. */
function ColumnSettings({ column, onChange }: { column: DataHubColumnViewColumnInput; onChange: (next: DataHubColumnViewColumnInput) => void }) {
    const { t } = useTranslation('entity.views');
    const display = displayFor(column);
    const spec = COLUMN_KIND_SPECS[column.type];
    const renderers = getRenderersFor(column.type);
    const patch = (p: Parameters<typeof withDisplay>[1]) => onChange(withDisplay(column, p));
    const widthPreset: WidthPreset =
        display.width == null ? 'M' : ((Object.keys(WIDTH_PRESETS) as (keyof typeof WIDTH_PRESETS)[]).find((k) => WIDTH_PRESETS[k] === display.width) ?? 'custom');
    const setRenderer = (key: string) =>
        patch({ custom: [...(display.custom || []).filter((e) => e.key !== RENDERER_CUSTOM_KEY), { key: RENDERER_CUSTOM_KEY, value: key }] });

    return (
        <Settings>
            {renderers.length > 1 && (
                <label>
                    {t('columnViews.display.format')}
                    <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={customDisplayValue(column, RENDERER_CUSTOM_KEY) || renderers[0].key}
                        options={renderers.map((r) => ({ value: r.key, label: t(r.label) }))}
                        onChange={setRenderer}
                    />
                </label>
            )}
            <label>
                {t('columnViews.display.width')}
                <Inline>
                    <Select<WidthPreset>
                        size="small"
                        style={{ flex: 1 }}
                        value={widthPreset}
                        options={[
                            { value: 'S', label: t('columnViews.display.widthS') },
                            { value: 'M', label: t('columnViews.display.widthM') },
                            { value: 'L', label: t('columnViews.display.widthL') },
                            { value: 'custom', label: t('columnViews.display.widthCustom') },
                        ]}
                        onChange={(v) => patch({ width: v === 'custom' ? display.width ?? spec.defaultWidth ?? WIDTH_PRESETS.M : WIDTH_PRESETS[v] })}
                    />
                    {widthPreset === 'custom' && (
                        <InputNumber size="small" min={spec.minWidth} max={2000} value={display.width ?? undefined} onChange={(v) => patch({ width: v == null ? null : Number(v) })} />
                    )}
                </Inline>
            </label>
            {spec.source === 'GRAPH' && (
                <>
                    <label>
                        {t('columnViews.display.labelStyle')}
                        <Select
                            size="small"
                            style={{ width: '100%' }}
                            value={display.labelStyle ?? DataHubColumnViewLabelStyle.FieldName}
                            options={[
                                { value: DataHubColumnViewLabelStyle.FieldName, label: t('columnViews.display.labelStyle.fieldName') },
                                { value: DataHubColumnViewLabelStyle.DatasetAndField, label: t('columnViews.display.labelStyle.datasetAndField') },
                                { value: DataHubColumnViewLabelStyle.FullPath, label: t('columnViews.display.labelStyle.fullPath') },
                            ]}
                            onChange={(v) => patch({ labelStyle: v })}
                        />
                    </label>
                    {!spec.singleValued && (
                        <>
                            <label>
                                {t('columnViews.display.maxItems')}
                                <InputNumber size="small" min={1} max={20} value={display.maxItems ?? undefined} onChange={(v) => patch({ maxItems: v == null ? null : Number(v) })} />
                            </label>
                            <label>
                                {t('columnViews.display.overflow')}
                                <Select
                                    size="small"
                                    style={{ width: '100%' }}
                                    value={display.overflow ?? DataHubColumnViewOverflow.Count}
                                    options={[
                                        { value: DataHubColumnViewOverflow.Count, label: t('columnViews.display.overflow.count') },
                                        { value: DataHubColumnViewOverflow.Ellipsis, label: t('columnViews.display.overflow.ellipsis') },
                                    ]}
                                    onChange={(v) => patch({ overflow: v })}
                                />
                            </label>
                            <label>
                                {t('columnViews.display.expand')}
                                <Select
                                    size="small"
                                    style={{ width: '100%' }}
                                    value={display.expand ?? DataHubColumnViewExpand.Popover}
                                    options={[
                                        { value: DataHubColumnViewExpand.Popover, label: t('columnViews.display.expand.popover') },
                                        { value: DataHubColumnViewExpand.Inline, label: t('columnViews.display.expand.inline') },
                                    ]}
                                    onChange={(v) => patch({ expand: v })}
                                />
                            </label>
                        </>
                    )}
                </>
            )}
        </Settings>
    );
}

function ShownRow({
    column,
    label,
    hint,
    onRemove,
    onMove,
    onChange,
    isFirst,
    isLast,
}: {
    column: DataHubColumnViewColumnInput;
    label: string;
    /** Secondary detail shown on hover (the urn behind a friendly structured-property / label name). */
    hint?: string;
    onRemove: () => void;
    onMove: (direction: MoveDirection) => void;
    onChange: (next: DataHubColumnViewColumnInput) => void;
    isFirst: boolean;
    isLast: boolean;
}) {
    const { t } = useTranslation('entity.views');
    const id = columnIdentity(column);
    const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id });
    return (
        <Row ref={setNodeRef} {...attributes} $isDragging={isDragging} $transform={CSS.Transform.toString(transform)} $transition={transition}>
            <DragHandle listeners={listeners} isDragging={isDragging} />
            <Typography.Text style={{ flex: 1 }} ellipsis={{ tooltip: hint ? `${label} — ${hint}` : label }} title={hint}>
                {label}
            </Typography.Text>
            <Tooltip title={t('columnViews.builder.moveTop')}>
                <Button size="small" type="text" icon={<VerticalAlignTopOutlined />} disabled={isFirst} onClick={() => onMove('top')} aria-label={t('columnViews.builder.moveTop')} />
            </Tooltip>
            <Tooltip title={t('columnViews.builder.moveUp')}>
                <Button size="small" type="text" icon={<ArrowUpOutlined />} disabled={isFirst} onClick={() => onMove('up')} aria-label={t('columnViews.builder.moveUp')} />
            </Tooltip>
            <Tooltip title={t('columnViews.builder.moveDown')}>
                <Button size="small" type="text" icon={<ArrowDownOutlined />} disabled={isLast} onClick={() => onMove('down')} aria-label={t('columnViews.builder.moveDown')} />
            </Tooltip>
            <Popover trigger="click" placement="left" content={<ColumnSettings column={column} onChange={onChange} />}>
                <Button size="small" type="text" icon={<SettingOutlined />} aria-label={t('columnViews.builder.settings')} />
            </Popover>
            <Button size="small" type="text" onClick={onRemove} aria-label={t('columnViews.builder.remove')}>
                ×
            </Button>
        </Row>
    );
}

/**
 * Two-pane definition editor (stacked on narrow viewports). Left "Available": Field attributes /
 * Structured properties / Labels / Relationships (six leaf kinds). Right "Shown": sortable list
 * with Name pinned, per-column settings gear and explicit reorder buttons. Then Sort by and Filters.
 */
export default function ColumnViewDefinitionBuilder({ state, updateState }: Props) {
    const { t } = useTranslation('entity.views');
    const { logicalModelsEnabled } = useAppConfig().config.featureFlags;
    const isNarrow = useIsNarrowViewport();
    const columns = state.definition?.columns || [];
    const ids = useMemo(() => columns.map(columnIdentity), [columns]);
    const graphCount = columns.filter(isGraphColumn).length;

    const setDefinition = (patch: Partial<NonNullable<ColumnViewBuilderState['definition']>>) =>
        updateState({ ...state, definition: { columns, sort: null, filter: null, ...state.definition, ...patch } });

    const setColumns = (next: DataHubColumnViewColumnInput[]) => {
        const sort = state.definition?.sort;
        const sortStillPresent = sort && next.some((c) => columnIdentity(c) === columnIdentity(sort.column));
        setDefinition({ columns: next, sort: sortStillPresent ? sort : null });
    };
    const has = (c: ColumnLike) => ids.includes(columnIdentity(c));
    const toggle = (c: DataHubColumnViewColumnInput) =>
        has(c) ? setColumns(columns.filter((x) => columnIdentity(x) !== columnIdentity(c))) : setColumns([...columns, c]);
    const remove = (c: ColumnLike) => setColumns(columns.filter((x) => columnIdentity(x) !== columnIdentity(c)));
    const onReorder = (newIds: string[]) => setColumns(newIds.map((id) => columns[ids.indexOf(id)]).filter(Boolean));
    const move = (c: ColumnLike, direction: MoveDirection) => onReorder(moveItem(ids, columnIdentity(c), direction));
    const replace = (c: ColumnLike, next: DataHubColumnViewColumnInput) =>
        setColumns(columns.map((x) => (columnIdentity(x) === columnIdentity(c) ? next : x)));
    // Builder columns are input-shaped ({ urn } only), so structured-property names come from a lookup.
    const propertyNames = useStructuredPropertyNames(columns);
    const label = (c: ColumnLike) =>
        columnHeader(c, t, { logicalModelsEnabled, structuredPropertyName: propertyNames[structuredPropertyUrnOf(c) ?? ''] });

    const filters = state.definition?.filter?.filters || [];
    const setFilters = (next: typeof filters) =>
        setDefinition({ filter: next.length ? { operator: state.definition?.filter?.operator || LogicalOperator.And, filters: next } : null });
    // Filter rows carry no id, so React keys are assigned per row object (setDefinition keeps the
    // untouched rows' identity) and carried over by `patch`, so editing a row never remounts its inputs.
    const filterKeys = useRef({ next: 0, byRow: new WeakMap<object, string>() });
    const filterKey = (row: object) => {
        let key = filterKeys.current.byRow.get(row);
        if (!key) {
            filterKeys.current.next += 1;
            key = `filter-${filterKeys.current.next}`;
            filterKeys.current.byRow.set(row, key);
        }
        return key;
    };

    /** Checkbox on wide screens; label + "Add" button on narrow ones. */
    const availableItem = (c: DataHubColumnViewColumnInput, text: React.ReactNode, disabledReason?: string) => {
        const checked = has(c);
        const disabled = !checked && Boolean(disabledReason);
        const control = isNarrow ? (
            <AvailableRow>
                <span>{text}</span>
                <Button size="small" disabled={disabled} onClick={() => toggle(c)}>
                    {checked ? t('columnViews.builder.remove') : t('columnViews.builder.add')}
                </Button>
            </AvailableRow>
        ) : (
            <Checkbox checked={checked} disabled={disabled} onChange={() => toggle(c)}>
                {text}
            </Checkbox>
        );
        return disabled ? <Tooltip title={disabledReason}>{control}</Tooltip> : control;
    };

    return (
        <div>
            <Panes $stacked={isNarrow}>
                <Pane>
                    <Typography.Text strong>{t('columnViews.builder.available')}</Typography.Text>
                    <Section>
                        <Typography.Text type="secondary">{t('columnViews.builder.attributes')}</Typography.Text>
                        {ATTRIBUTE_KINDS.map((kind) => (
                            <div key={kind}>{availableItem({ type: kind }, t(COLUMN_KIND_SPECS[kind].labelKey))}</div>
                        ))}
                    </Section>
                    <Section>
                        <Typography.Text type="secondary">{t('columnViews.builder.structuredProperties')}</Typography.Text>
                        <StructuredPropertyPicker
                            placeholder={t('columnViews.builder.search')}
                            selectedUrns={columns.filter(isStructuredPropertyColumn).map((c) => c.structuredPropertyParams?.urn as string)}
                            onChange={(urns) => {
                                // Urns still selected keep their column (and its display); only new urns get a fresh one.
                                const current = columns.filter(isStructuredPropertyColumn);
                                setColumns([
                                    ...columns.filter((c) => !isStructuredPropertyColumn(c)),
                                    ...urns.map(
                                        (urn) =>
                                            current.find((c) => structuredPropertyUrnOf(c) === urn) ?? {
                                                type: DataHubColumnViewColumnType.StructuredProperty,
                                                structuredPropertyParams: { urn },
                                            },
                                    ),
                                ]);
                            }}
                        />
                    </Section>
                    <Section>
                        <Typography.Text type="secondary">{t('columnViews.builder.labels')}</Typography.Text>
                        {columns.filter(isLabelColumn).map((c) => (
                            <div key={columnIdentity(c)}>
                                <Checkbox checked onChange={() => remove(c)}>
                                    {label(c)}
                                </Checkbox>
                            </div>
                        ))}
                        <LabelPicker
                            placeholder={t('columnViews.builder.addLabel')}
                            onPick={(entity) =>
                                // `label` rides along (not part of the input type) so the header shows the
                                // picked name before save; toColumnInput strips it on submit.
                                toggle({
                                    type: DataHubColumnViewColumnType.Label,
                                    labelParams: { urn: entity.urn, label: entity } as any,
                                })
                            }
                        />
                    </Section>
                    <Section>
                        <Typography.Text type="secondary">{t('columnViews.builder.relationships')}</Typography.Text>
                        {availableRelationshipKinds(Boolean(logicalModelsEnabled)).map((spec) => (
                            <div key={spec.kind}>
                                {availableItem(
                                    { type: spec.kind },
                                    <>
                                        {t(spec.labelKey)}
                                        {spec.highFanout && (
                                            <Tooltip title={t('columnViews.relationship.highFanoutHint')}>
                                                <Hint type="secondary">{t('columnViews.relationship.highFanout')}</Hint>
                                            </Tooltip>
                                        )}
                                    </>,
                                    graphCount >= MAX_GRAPH_COLUMNS ? t('columnViews.relationship.maxReached', { max: MAX_GRAPH_COLUMNS }) : undefined,
                                )}
                            </div>
                        ))}
                    </Section>
                </Pane>
                <Pane>
                    <Typography.Text strong>{t('columnViews.builder.shown')}</Typography.Text>
                    <Row>
                        <Typography.Text type="secondary">{t('columnViews.nameColumn')}</Typography.Text>
                    </Row>
                    <ColumnViewSortableList items={ids} onChange={onReorder}>
                        {columns.map((c, i) => (
                            <ShownRow
                                key={columnIdentity(c)}
                                column={c}
                                label={label(c)}
                                hint={columnHeaderHint(c, label(c))}
                                isFirst={i === 0}
                                isLast={i === columns.length - 1}
                                onRemove={() => remove(c)}
                                onMove={(d) => move(c, d)}
                                onChange={(next) => replace(c, next)}
                            />
                        ))}
                    </ColumnViewSortableList>
                </Pane>
            </Panes>
            <Section>
                <Typography.Text strong>{t('columnViews.builder.sortBy')}</Typography.Text>
                <Inline>
                    <Select
                        allowClear
                        style={{ flex: 1 }}
                        value={state.definition?.sort ? columnIdentity(state.definition.sort.column) : undefined}
                        options={columns.map((c) => ({
                            value: columnIdentity(c),
                            label: label(c),
                            disabled: !isSortableColumn(c),
                            title: isSortableColumn(c) ? undefined : t('columnViews.relationship.notSortable'),
                        }))}
                        onChange={(id) =>
                            setDefinition({
                                sort: id ? { column: columns[ids.indexOf(id)], order: state.definition?.sort?.order || SortOrder.Ascending } : null,
                            })
                        }
                    />
                    {state.definition?.sort && (
                        <Select
                            style={{ width: 140 }}
                            value={state.definition.sort.order}
                            options={[
                                { value: SortOrder.Ascending, label: SortOrder.Ascending },
                                { value: SortOrder.Descending, label: SortOrder.Descending },
                            ]}
                            onChange={(order) => setDefinition({ sort: { ...state.definition!.sort!, order } })}
                        />
                    )}
                </Inline>
            </Section>
            <Section>
                <Typography.Text strong>{t('columnViews.builder.filters')}</Typography.Text>
                {filters.map((f, i) => {
                    const op = OPERATORS.find((o) => o.value === (f.negated && f.condition === 'EXISTS' ? 'NOT_EXISTS' : f.condition)) || OPERATORS[0];
                    const patch = (p: Partial<typeof f>) =>
                        setFilters(
                            filters.map((x, j) => {
                                if (j !== i) return x;
                                const next = { ...x, ...p };
                                filterKeys.current.byRow.set(next, filterKey(x));
                                return next;
                            }),
                        );
                    // Built-in fields plus `structuredProperties.<urn>` for every property column in Shown.
                    const fieldOptions = [
                        ...FILTER_FIELDS.map((v) => ({ value: v, label: v })),
                        ...columns.filter(isStructuredPropertyColumn).map((c) => ({
                            value: STRUCTURED_PROPERTY_FILTER_PREFIX + structuredPropertyUrnOf(c),
                            label: label(c),
                        })),
                    ];
                    return (
                        <Inline key={filterKey(f)}>
                            <Select
                                showSearch
                                style={{ width: 160 }}
                                value={f.field}
                                options={fieldOptions}
                                // Urn values do not carry over between label and text fields.
                                onChange={(field) => patch({ field, values: isLabelField(field) === isLabelField(f.field) ? f.values : [] })}
                            />
                            <Select
                                style={{ width: 160 }}
                                value={op.value}
                                // Ordering label urns is meaningless (filterSchemaRowsByView treats it as neutral).
                                options={OPERATORS.filter((o) => !(o.ordered && isLabelField(f.field))).map((o) => ({ value: o.value, label: t(o.labelKey) }))}
                                onChange={(v) => {
                                    const o = OPERATORS.find((x) => x.value === v)!;
                                    patch({ condition: (o.negated ? 'EXISTS' : o.value) as FilterOperator, negated: Boolean(o.negated) });
                                }}
                            />
                            {!op.unary &&
                                (isLabelField(f.field) ? (
                                    <div style={{ flex: 1 }}>
                                        <LabelPicker
                                            size="middle"
                                            placeholder={t('columnViews.builder.addLabel')}
                                            selectedUrns={f.values || []}
                                            onChangeUrns={(urns) => patch({ values: urns })}
                                        />
                                    </div>
                                ) : (
                                    <Input
                                        style={{ flex: 1 }}
                                        value={(f.values || []).join(',')}
                                        onChange={(e) => patch({ values: e.target.value.split(',').map((s) => s.trim()).filter(Boolean) })}
                                    />
                                ))}
                            <Button size="small" type="text" onClick={() => setFilters(filters.filter((_, j) => j !== i))}>
                                ×
                            </Button>
                        </Inline>
                    );
                })}
                <Button type="link" size="small" onClick={() => setFilters([...filters, { field: 'tags', condition: 'EQUAL' as FilterOperator, values: [], negated: false }])}>
                    {t('columnViews.addFilter')}
                </Button>
            </Section>
        </div>
    );
}
