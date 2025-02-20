import React, { useMemo, useState } from 'react';
import { ColumnsType } from 'antd/es/table';
import { Button, Table } from 'antd';
import styled from 'styled-components';
import { useDebounce } from 'react-use';

import { FixedType } from 'rc-table/lib/interface';
import translateFieldPath from '@src/app/entityV2/dataset/profile/schema/utils/translateFieldPath';
import {
    EditableSchemaMetadata,
    EntityType,
    SchemaField,
    SchemaMetadata,
    UsageQueryResult,
} from '../../../../../../types.generated';
import useSchemaTitleRenderer from '../../../../dataset/profile/schema/utils/schemaTitleRenderer';
import { ExtendedSchemaFields } from '../../../../dataset/profile/schema/utils/types';
import useDescriptionRenderer from './utils/useDescriptionRenderer';
import useUsageStatsRenderer from './utils/useUsageStatsRenderer';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { REDESIGN_COLORS } from '../../../constants';
import ExpandIcon from './components/ExpandIcon';
import SchemaFieldDrawer from './components/SchemaFieldDrawer/SchemaFieldDrawer';
import useKeyboardControls from './useKeyboardControls';
import useExtractFieldDescriptionInfo from './utils/useExtractFieldDescriptionInfo';

export type Props = {
    rows: Array<ExtendedSchemaFields>;
    schemaMetadata: SchemaMetadata | undefined | null;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    usageStats?: UsageQueryResult | null;
    fullHeight?: boolean;
    expandedRowsFromFilter?: Set<string>;
    expandedDrawerFieldPath: string | null;
    setExpandedDrawerFieldPath: (path: string | null) => void;
    openTimelineDrawer?: boolean;
    setOpenTimelineDrawer?: any;
    inputFields?: SchemaField[];
    refetch?: () => void;
};

const TableContainer = styled.div<{ fullHeight?: boolean }>`
    margin-top: ${(props) => (props.fullHeight ? '0px' : '5px')};
    margin-left: ${(props) => (props.fullHeight ? '12px' : '0px')};
    .ant-table-thead > tr > th {
        background-color: transparent;
        font-weight: 600;
        color: ${REDESIGN_COLORS.DARK_GREY};
        font-weight: 700;
    }
    &&& .ant-table-cell:first-of-type {
        ${(props) => !props.fullHeight && 'padding: 8px 8px 8px 0px'};
    }

    &&& .selected-row * {
        color: white;
        background-color: ${REDESIGN_COLORS.BACKGROUND_PURPLE};
    }

    &&& .field-column {
        max-width: 100px;
    }

    &&& .description-column {
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        max-width: 100px;
    }
`;

const StyledButton = styled(Button)`
    color: #b0a2c2;
    font-weight: 500;
    :hover {
        color: ${REDESIGN_COLORS.DARK_GREY};
    }
`;

const KEYBOARD_CONTROL_DEBOUNCE_MS = 50;

export default function CompactSchemaTable({
    rows,
    schemaMetadata,
    editableSchemaMetadata,
    usageStats,
    fullHeight,
    inputFields,
    expandedDrawerFieldPath,
    setExpandedDrawerFieldPath,
    openTimelineDrawer = false,
    setOpenTimelineDrawer,
    refetch,
}: Props): JSX.Element {
    const numberOfRowsToShow = fullHeight ? 20 : 5;
    const { urn } = useEntityData();
    const entityRegistry = useEntityRegistry();

    const hasUsageStats = useMemo(() => (usageStats?.aggregations?.fields?.length || 0) > 0, [usageStats]);

    const descriptionRender = useDescriptionRenderer(editableSchemaMetadata, true);
    const usageStatsRenderer = useUsageStatsRenderer(usageStats);

    const schemaTitleRenderer = useSchemaTitleRenderer(urn, schemaMetadata, '', true);

    const shortenedRows = useMemo(() => rows.slice(0, numberOfRowsToShow), [rows, numberOfRowsToShow]);
    const hasSeeMore = rows.length > numberOfRowsToShow;

    const schemaFields = schemaMetadata ? schemaMetadata.fields : inputFields;
    const [schemaFieldDrawerFieldPath, setSchemaFieldDrawerFieldPath] = useState(expandedDrawerFieldPath);
    useDebounce(() => setSchemaFieldDrawerFieldPath(expandedDrawerFieldPath), KEYBOARD_CONTROL_DEBOUNCE_MS, [
        expandedDrawerFieldPath,
    ]);

    const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

    const { selectPreviousField, selectNextField } = useKeyboardControls(
        shortenedRows,
        expandedDrawerFieldPath,
        setExpandedDrawerFieldPath,
        expandedRows,
        setExpandedRows,
    );

    const extractFieldDescription = useExtractFieldDescriptionInfo(editableSchemaMetadata);

    const fieldColumn = {
        fixed: 'left' as FixedType,
        width: 100,
        title: 'Name',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        className: 'field-column',
        render: schemaTitleRenderer,
        filtered: true,
        onCell: () => ({
            style: {
                whiteSpace: 'pre' as any,
            },
        }),
        sorter: (sourceA, sourceB) =>
            translateFieldPath(sourceA.fieldPath).localeCompare(translateFieldPath(sourceB.fieldPath)),
    };

    const descriptionColumn = {
        ellipsis: true,
        width: 600,
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        className: 'description-column',
        render: descriptionRender,
        onCell: () => ({
            style: {
                whiteSpace: 'pre' as any,
                textWrap: 'whitespace',
            },
        }),
        sorter: (sourceA, sourceB) =>
            (extractFieldDescription(sourceA).sanitizedDescription ? 1 : 0) -
            (extractFieldDescription(sourceB).sanitizedDescription ? 1 : 0),
    };

    // Function to get the count of each usageStats fieldPath
    function getCount(fieldPath: any) {
        const data: any =
            usageStats?.aggregations?.fields &&
            usageStats?.aggregations?.fields?.find((field) => {
                return field?.fieldName === fieldPath;
            });
        return (data && data.count) ?? 0;
    }

    const usageColumn = {
        width: '100',
        title: 'Usage',
        dataIndex: 'fieldPath',
        key: 'usage',
        render: usageStatsRenderer,
        sorter: (sourceA, sourceB) => getCount(sourceA.fieldPath) - getCount(sourceB.fieldPath),
    };

    let allColumns: ColumnsType<ExtendedSchemaFields> = [fieldColumn, descriptionColumn];

    if (hasUsageStats) {
        allColumns = [...allColumns, usageColumn];
    }

    const rowClassName = (record) => {
        let className = '';

        if (expandedDrawerFieldPath === record.fieldPath) {
            className += 'selected-row';
        } else {
            className = 'row';
        }

        return className;
    };

    return (
        <TableContainer fullHeight={fullHeight}>
            <Table
                size="small"
                columns={allColumns}
                dataSource={shortenedRows}
                rowKey="fieldPath"
                pagination={false}
                rowClassName={rowClassName}
                onRow={(record) => ({
                    id: `column-${record.fieldPath}`,
                })}
                scroll={{ x: 'auto' }}
                expandable={{
                    expandIcon: (props) => <ExpandIcon {...props} isCompact />,
                }}
            />
            {hasSeeMore && (
                <StyledButton type="text" size="small" href={entityRegistry.getEntityUrl(EntityType.Dataset, urn)}>
                    View {rows.length - numberOfRowsToShow} More
                </StyledButton>
            )}
            {!!schemaFields && (
                <SchemaFieldDrawer
                    schemaFields={schemaFields}
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
