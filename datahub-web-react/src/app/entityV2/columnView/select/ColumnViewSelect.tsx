import { useApolloClient } from '@apollo/client';
import { ArrowDownOutlined, ArrowUpOutlined, VerticalAlignTopOutlined } from '@ant-design/icons';
import { Button, Checkbox, Divider, Drawer, Popover, Tag, Tooltip, Typography, message } from 'antd';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import ColumnViewBuilderModal, { columnViewToBuilderState } from '@app/entityV2/columnView/builder/ColumnViewBuilderModal';
import StructuredPropertyPicker from '@app/entityV2/columnView/builder/StructuredPropertyPicker';
import { updateColumnViewSelectCache } from '@app/entityV2/columnView/cacheUtils';
import { useColumnViewContext } from '@app/entityV2/columnView/ColumnViewContext';
import {
    BUILT_IN_DEFAULT_COLUMNS,
    ColumnLike,
    OPTIONAL_ATTRIBUTE_COLUMNS,
    columnHeader,
    columnHeaderHint,
    columnIdentity,
    isStructuredPropertyColumn,
    structuredPropertyUrnOf,
    toColumnInput,
} from '@app/entityV2/columnView/columnKinds';
import ColumnViewSortableList, { MoveDirection, moveItem } from '@app/entityV2/columnView/ColumnViewSortableList';
import ColumnViewDropdownMenu from '@app/entityV2/columnView/menu/ColumnViewDropdownMenu';
import { ColumnViewBuilderState, DEFAULT_LIST_COLUMN_VIEWS_PAGE_SIZE, SCHEMA_TARGET } from '@app/entityV2/columnView/types';
import { useIsNarrowViewport } from '@app/entityV2/columnView/useIsNarrowViewport';
import { useStructuredPropertyNames } from '@app/entityV2/columnView/useStructuredPropertyNames';
import { useAppConfig } from '@app/useAppConfig';

import {
    useListGlobalColumnViewsQuery,
    useListMyColumnViewsQuery,
    useUpdateColumnViewMutation,
} from '@graphql/columnView.generated';
import { DataHubColumnView, DataHubColumnViewColumn, DataHubColumnViewColumnType, DataHubColumnViewDefinition, DataHubViewType } from '@types';

const Panel = styled.div`
    width: 360px;
    max-height: 520px;
    overflow: auto;
`;

const Row = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 4px 0;
`;

const Heading = styled(Typography.Text)`
    display: block;
    margin: 8px 0 4px;
    font-size: 11px;
    letter-spacing: 0.04em;
`;

const Actions = styled.div`
    display: flex;
    gap: 8px;
    margin-top: 8px;
`;

/**
 * The single "Columns" control in the Schema-tab toolbar. Button label `Columns: <Default | view>`
 * with a • when ad hoc modified. Popover zones: (a) ad hoc checkbox + drag list of the currently
 * available columns (Name locked at top) + add structured property + Save as / Update / Reset;
 * (b) saved views MY COLUMN VIEWS / PUBLIC with default badges, per-row menu, + Create.
 */
export default function ColumnViewSelect() {
    const { t } = useTranslation('entity.views');
    const client = useApolloClient();
    const userContext = useUserContext();
    const { logicalModelsEnabled } = useAppConfig().config.featureFlags;
    const { selectedColumnView, defaultColumnView, activeDefinition, isAdHocModified, selectedUrn, setSelectedUrn, setAdHocDefinition } =
        useColumnViewContext();
    const [editing, setEditing] = useState<ColumnViewBuilderState | undefined>(undefined);
    const [editingUrn, setEditingUrn] = useState<string | undefined>(undefined);
    const [sheetOpen, setSheetOpen] = useState(false);
    const [popoverOpen, setPopoverOpen] = useState(false);
    // The builder modal replaces the chooser: close the popover / sheet so it isn't left open
    // behind the modal (and so the next click on the Columns button opens rather than toggles).
    const openBuilder = (state: ColumnViewBuilderState, urn?: string) => {
        setPopoverOpen(false);
        setSheetOpen(false);
        setEditingUrn(urn);
        setEditing(state);
    };
    const isNarrow = useIsNarrowViewport();
    const [update] = useUpdateColumnViewMutation();

    const { data: mine } = useListMyColumnViewsQuery({
        variables: { start: 0, count: DEFAULT_LIST_COLUMN_VIEWS_PAGE_SIZE, viewType: DataHubViewType.Personal, target: SCHEMA_TARGET },
        fetchPolicy: 'cache-first',
    });
    const { data: global } = useListGlobalColumnViewsQuery({
        variables: { start: 0, count: DEFAULT_LIST_COLUMN_VIEWS_PAGE_SIZE, target: SCHEMA_TARGET },
        fetchPolicy: 'cache-first',
    });

    // Columns currently shown (the built-in default when no view is active) and the pool of
    // available ones (shown ∪ built-in ∪ Business Attribute); toggling moves between them.
    const shown: ColumnLike[] = useMemo(
        () => (activeDefinition?.columns as ColumnLike[]) || BUILT_IN_DEFAULT_COLUMNS,
        [activeDefinition],
    );
    const available: ColumnLike[] = useMemo(() => {
        const pool = [...shown, ...BUILT_IN_DEFAULT_COLUMNS, ...OPTIONAL_ATTRIBUTE_COLUMNS];
        const seen = new Set<string>();
        return pool.filter((c) => !seen.has(columnIdentity(c)) && seen.add(columnIdentity(c)));
    }, [shown]);
    const shownIds = shown.map(columnIdentity);

    // Checkbox labels: friendly names (never a bare urn when one can be resolved), urn on hover.
    const propertyNames = useStructuredPropertyNames(available);
    const columnLabel = (c: ColumnLike) => {
        const header = columnHeader(c, t, {
            logicalModelsEnabled,
            structuredPropertyName: propertyNames[structuredPropertyUrnOf(c) ?? ''],
        });
        const hint = columnHeaderHint(c, header);
        return hint ? <Tooltip title={hint}>{header}</Tooltip> : header;
    };

    const setColumns = (columns: ColumnLike[]) =>
        setAdHocDefinition({
            ...(activeDefinition || { sort: null, filter: null }),
            columns: columns as DataHubColumnViewColumn[],
        } as DataHubColumnViewDefinition);
    const toggle = (c: ColumnLike) =>
        setColumns(shownIds.includes(columnIdentity(c)) ? shown.filter((s) => columnIdentity(s) !== columnIdentity(c)) : [...shown, c]);
    const reorder = (ids: string[]) => setColumns(ids.map((id) => shown[shownIds.indexOf(id)]).filter(Boolean));
    const move = (c: ColumnLike, direction: MoveDirection) => reorder(moveItem(shownIds, columnIdentity(c), direction));

    const label = selectedColumnView?.name || t('columnViews.builtInDefault');
    const canUpdateSelected =
        selectedColumnView &&
        (selectedColumnView.viewType === DataHubViewType.Global
            ? Boolean(userContext.platformPrivileges?.manageGlobalViews)
            : selectedColumnView.created?.actor === userContext.user?.urn);

    const adHocBuilderState = (): ColumnViewBuilderState => ({
        viewType: DataHubViewType.Personal,
        name: '',
        description: null,
        target: SCHEMA_TARGET,
        definition: {
            columns: shown.map(toColumnInput),
            sort: activeDefinition?.sort ? { column: toColumnInput(activeDefinition.sort.column), order: activeDefinition.sort.order } : null,
            filter: activeDefinition?.filter ?? null,
        },
    });

    const updateSelected = () =>
        selectedColumnView &&
        update({
            variables: { urn: selectedColumnView.urn, input: { definition: adHocBuilderState().definition as any } },
        })
            .then((r) => {
                const v = r.data?.updateColumnView as DataHubColumnView;
                if (v) updateColumnViewSelectCache(v.urn, v, client);
                setAdHocDefinition(undefined);
            })
            .catch((e) => message.error(e.message));

    const renderView = (v: DataHubColumnView) => (
        <Row key={v.urn}>
            <Typography.Link onClick={() => setSelectedUrn(v.urn)} strong={v.urn === selectedColumnView?.urn}>
                {v.name}
            </Typography.Link>
            <span>
                {v.urn === defaultColumnView?.urn && <Tag>{t('columnViews.defaultBadge')}</Tag>}
                <ColumnViewDropdownMenu
                    view={v}
                    onEdit={() => openBuilder(columnViewToBuilderState(v), v.urn)}
                />
            </span>
        </Row>
    );

    const content = (
        <Panel>
            <Heading type="secondary">{t('columnViews.builder.shown').toUpperCase()}</Heading>
            <Row>
                <Checkbox checked disabled>
                    {t('columnViews.nameColumn')}
                </Checkbox>
            </Row>
            <ColumnViewSortableList items={shownIds} onChange={reorder}>
                {shown.map((c, i) => (
                    <Row key={columnIdentity(c)}>
                        <Checkbox checked onChange={() => toggle(c)}>
                            {columnLabel(c)}
                        </Checkbox>
                        <span>
                            <Button size="small" type="text" icon={<VerticalAlignTopOutlined />} disabled={i === 0} onClick={() => move(c, 'top')} aria-label={t('columnViews.builder.moveTop')} />
                            <Button size="small" type="text" icon={<ArrowUpOutlined />} disabled={i === 0} onClick={() => move(c, 'up')} aria-label={t('columnViews.builder.moveUp')} />
                            <Button size="small" type="text" icon={<ArrowDownOutlined />} disabled={i === shown.length - 1} onClick={() => move(c, 'down')} aria-label={t('columnViews.builder.moveDown')} />
                        </span>
                    </Row>
                ))}
            </ColumnViewSortableList>
            {available
                .filter((c) => !shownIds.includes(columnIdentity(c)))
                .map((c) => (
                    <Row key={columnIdentity(c)}>
                        <Checkbox checked={false} onChange={() => toggle(c)}>
                            {columnLabel(c)}
                        </Checkbox>
                    </Row>
                ))}
            <StructuredPropertyPicker
                placeholder={t('columnViews.addStructuredProperty')}
                selectedUrns={shown.filter(isStructuredPropertyColumn).map((c) => c.structuredPropertyParams?.structuredProperty?.urn as string)}
                onChange={(urns, entities) =>
                    setColumns([
                        ...shown.filter((c) => !isStructuredPropertyColumn(c)),
                        ...urns.map((urn) => ({
                            type: DataHubColumnViewColumnType.StructuredProperty,
                            structuredPropertyParams: { structuredProperty: entities?.find((e) => e.urn === urn) || { urn } },
                        })),
                    ] as ColumnLike[])
                }
            />
            <Actions>
                <Button size="small" onClick={() => openBuilder(adHocBuilderState())}>
                    {t('columnViews.saveAs')}
                </Button>
                {isAdHocModified && canUpdateSelected && (
                    <Button size="small" onClick={updateSelected}>
                        {t('columnViews.update', { name: selectedColumnView?.name })}
                    </Button>
                )}
                {isAdHocModified && (
                    <Button size="small" type="link" onClick={() => setAdHocDefinition(undefined)}>
                        {t('columnViews.reset')}
                    </Button>
                )}
            </Actions>
            <Divider style={{ margin: '8px 0' }} />
            <Row>
                <Typography.Link onClick={() => setSelectedUrn(null)} strong={!selectedColumnView}>
                    {t('columnViews.builtInDefault')}
                </Typography.Link>
            </Row>
            <Heading type="secondary">{t('columnViews.myViews')}</Heading>
            {(mine?.listMyColumnViews?.columnViews || []).map((v) => renderView(v as DataHubColumnView))}
            <Heading type="secondary">{t('columnViews.publicViews')}</Heading>
            {(global?.listGlobalColumnViews?.columnViews || []).map((v) => renderView(v as DataHubColumnView))}
            <Button type="link" onClick={() => openBuilder(adHocBuilderState())}>
                + {t('columnViews.create')}
            </Button>
        </Panel>
    );

    return (
        <>
            {isNarrow ? (
                <>
                    <Button data-testid="column-view-select" size="small" onClick={() => setSheetOpen(true)}>
                        {t('columnViews.columnsButton', { name: label })}
                        {isAdHocModified && ' •'}
                    </Button>
                    <Drawer placement="bottom" height="80vh" open={sheetOpen} onClose={() => setSheetOpen(false)} title={t('columnViews.settingsColumns')}>
                        {content}
                    </Drawer>
                </>
            ) : (
                <Popover content={content} trigger="click" placement="bottomRight" open={popoverOpen} onOpenChange={setPopoverOpen}>
                    <Button data-testid="column-view-select" size="small">
                        {t('columnViews.columnsButton', { name: label })}
                        {isAdHocModified && ' •'}
                    </Button>
                </Popover>
            )}
            {editing && (
                <ColumnViewBuilderModal
                    urn={editingUrn}
                    initialState={editing}
                    onSubmit={(v) => {
                        setSelectedUrn(v.urn);
                        setEditing(undefined);
                        setEditingUrn(undefined);
                    }}
                    onCancel={() => {
                        setEditing(undefined);
                        setEditingUrn(undefined);
                    }}
                />
            )}
            {/* selectedUrn is read for the tri-state contract; undefined = default not yet applied */}
            {selectedUrn === undefined && null}
        </>
    );
}
