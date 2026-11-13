import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import { Button } from '@components/components/Button';
import { Table } from '@components/components/Table';
import { Column, TableTreeDataProps } from '@components/components/Table/types';

import useSchemaTitleRenderer from '@app/entityV2/dataset/profile/schema/utils/schemaTitleRenderer';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
import { useEntityData } from '@app/entityV2/shared/EntityContext';
import SchemaFieldDrawer from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/SchemaFieldDrawer';
import useKeyboardControls from '@app/entityV2/shared/tabs/Dataset/Schema/useKeyboardControls';
import { getFieldUsageCount } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/statsUtil';
import useDescriptionRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useDescriptionRenderer';
import useEditableSchemaFieldInfoMaps from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useEditableSchemaFieldInfoMaps';
import useExtractFieldDescriptionInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldDescriptionInfo';
import useUsageStatsRenderer from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useUsageStatsRenderer';
import { useEntityRegistry } from '@app/useEntityRegistry';
import translateFieldPath from '@src/app/entityV2/dataset/profile/schema/utils/translateFieldPath';

import { EditableSchemaMetadata, EntityType, SchemaField, SchemaMetadata, UsageQueryResult } from '@types';

type Props = {
    rows: Array<ExtendedSchemaFields>;
    schemaMetadata: SchemaMetadata | undefined | null;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    editedPrimaryKeys?: string[] | null;
    usageStats?: UsageQueryResult | null;
    fullHeight?: boolean;
    expandedDrawerFieldPath: string | null;
    setExpandedDrawerFieldPath: (path: string | null) => void;
    openTimelineDrawer?: boolean;
    setOpenTimelineDrawer?: any;
    inputFields?: SchemaField[];
    refetch?: () => void;
};

// Column so "View more" sits under the table, left-aligned; the table itself fills the width.
const TableContainer = styled.div<{ fullHeight?: boolean }>`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 4px;
    margin-top: ${(props) => (props.fullHeight ? '0px' : '5px')};
    padding: ${(props) => (props.fullHeight ? '12px' : '0')};
`;

const KEYBOARD_CONTROL_DEBOUNCE_MS = 50;

export default function CompactSchemaTable({
    rows,
    schemaMetadata,
    editableSchemaMetadata,
    editedPrimaryKeys,
    usageStats,
    fullHeight,
    inputFields,
    expandedDrawerFieldPath,
    setExpandedDrawerFieldPath,
    openTimelineDrawer = false,
    setOpenTimelineDrawer,
    refetch,
}: Props): JSX.Element {
    const { t } = useTranslation('entity.profile.schema');
    const { t: tc } = useTranslation('common.labels');
    const numberOfRowsToShow = fullHeight ? 20 : 5;
    const { urn } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();

    const hasUsageStats = useMemo(() => (usageStats?.aggregations?.fields?.length || 0) > 0, [usageStats]);

    const fieldInfoMaps = useEditableSchemaFieldInfoMaps(editableSchemaMetadata);
    const descriptionRender = useDescriptionRenderer(editableSchemaMetadata, true, fieldInfoMaps);
    const usageStatsRenderer = useUsageStatsRenderer(usageStats);

    const schemaTitleRenderer = useSchemaTitleRenderer(urn, schemaMetadata, editedPrimaryKeys, '', true);

    const shortenedRows = useMemo(() => rows.slice(0, numberOfRowsToShow), [rows, numberOfRowsToShow]);
    const hasSeeMore = rows.length > numberOfRowsToShow;

    const schemaFields = schemaMetadata ? schemaMetadata.fields : inputFields;
    const [schemaFieldDrawerFieldPath, setSchemaFieldDrawerFieldPath] = useState(expandedDrawerFieldPath);
    useDebounce(() => setSchemaFieldDrawerFieldPath(expandedDrawerFieldPath), KEYBOARD_CONTROL_DEBOUNCE_MS, [
        expandedDrawerFieldPath,
    ]);

    // Shared with the keyboard controls, so ArrowRight/ArrowLeft expand and collapse the rows on screen.
    const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
    // Memoized: whenever this object changes, the table re-sorts and re-flattens the whole field tree.
    const treeData = useMemo(
        (): TableTreeDataProps<ExtendedSchemaFields> => ({
            getChildren: (field) => field.children,
            expandedRowKeys: Array.from(expandedRows),
            onExpandedRowsChange: (keys) => setExpandedRows(new Set(keys)),
        }),
        [expandedRows],
    );

    const { selectPreviousField, selectNextField } = useKeyboardControls(
        shortenedRows,
        expandedDrawerFieldPath,
        setExpandedDrawerFieldPath,
        expandedRows,
        setExpandedRows,
    );

    const extractFieldDescription = useExtractFieldDescriptionInfo(editableSchemaMetadata, fieldInfoMaps);

    // Memoized so the table's memoized cells only re-render when a row or a renderer actually changes;
    // this table sits in sidebars whose parents re-render often.
    const columns = useMemo(() => {
        const tableColumns: Column<ExtendedSchemaFields>[] = [
            {
                title: tc('name'),
                key: 'fieldPath',
                width: '100px',
                render: (record) => schemaTitleRenderer(record.fieldPath, record),
                sorter: (sourceA, sourceB) =>
                    translateFieldPath(sourceA.fieldPath).localeCompare(translateFieldPath(sourceB.fieldPath)),
            },
            {
                title: tc('description'),
                key: 'description',
                render: (record) => descriptionRender(record.description ?? undefined, record),
                sorter: (sourceA, sourceB) =>
                    (extractFieldDescription(sourceA).sanitizedDescription ? 1 : 0) -
                    (extractFieldDescription(sourceB).sanitizedDescription ? 1 : 0),
            },
        ];
        if (hasUsageStats) {
            tableColumns.push({
                title: tc('usage'),
                key: 'usage',
                width: '100px',
                render: (record) => usageStatsRenderer(record.fieldPath),
                sorter: (sourceA, sourceB) =>
                    getFieldUsageCount(usageStats, sourceA.fieldPath) -
                    getFieldUsageCount(usageStats, sourceB.fieldPath),
            });
        }
        return tableColumns;
    }, [
        tc,
        schemaTitleRenderer,
        descriptionRender,
        extractFieldDescription,
        hasUsageStats,
        usageStatsRenderer,
        usageStats,
    ]);

    return (
        <TableContainer fullHeight={fullHeight}>
            <Table<ExtendedSchemaFields>
                data-testid="compact-schema-table"
                columns={columns}
                data={shortenedRows}
                rowKey="fieldPath"
                size="sm"
                isScrollable
                memoizeCells
                treeData={treeData}
                focusedRowKey={expandedDrawerFieldPath}
                rowDataTestId={(record) => `compact-column-${record.fieldPath}`}
            />
            {hasSeeMore && (
                <Button
                    variant="text"
                    color="gray"
                    size="sm"
                    onClick={() => history.push(entityRegistry.getEntityUrl(EntityType.Dataset, urn))}
                >
                    {t('compactSchemaTable.viewMore', { count: rows.length - numberOfRowsToShow })}
                </Button>
            )}
            {!!schemaFields && (
                <SchemaFieldDrawer
                    schemaFields={schemaFields}
                    schemaPrimaryKeys={schemaMetadata?.primaryKeys}
                    editedPrimaryKeys={editedPrimaryKeys}
                    expandedDrawerFieldPath={schemaFieldDrawerFieldPath}
                    editableSchemaMetadata={editableSchemaMetadata}
                    setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                    openTimelineDrawer={openTimelineDrawer}
                    setOpenTimelineDrawer={setOpenTimelineDrawer}
                    selectPreviousField={selectPreviousField}
                    selectNextField={selectNextField}
                    usageStats={usageStats}
                    displayedRows={shortenedRows}
                    refetch={refetch}
                    mask
                />
            )}
        </TableContainer>
    );
}
