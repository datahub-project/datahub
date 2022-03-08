import React from 'react';
import { Button, Typography } from 'antd';
import { FileTextOutlined, TableOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import CustomPagination from './CustomPagination';
import TabToolbar from '../../../../shared/components/styled/TabToolbar';

const SchemaHeaderContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    padding-bottom: 16px;
`;

// TODO(Gabe): undo display: none when dbt/bigquery flickering has been resolved
const ShowVersionButton = styled(Button)`
    display: inline-block;
    margin-right: 10px;
    display: none;
`;

const KeyButton = styled(Button)<{ $highlighted: boolean }>`
    border-radius: 8px 0px 0px 8px;
    font-weight: ${(props) => (props.$highlighted ? '600' : '400')};
`;

const ValueButton = styled(Button)<{ $highlighted: boolean }>`
    border-radius: 0px 8px 8px 0px;
    font-weight: ${(props) => (props.$highlighted ? '600' : '400')};
`;

const KeyValueButtonGroup = styled.div`
    margin-right: 10px;
    display: inline-block;
`;

type Props = {
    maxVersion?: number;
    fetchVersions?: (version1: number, version2: number) => void;
    editMode: boolean;
    setEditMode?: (mode: boolean) => void;
    hasRaw: boolean;
    showRaw: boolean;
    setShowRaw: (show: boolean) => void;
    hasKeySchema: boolean;
    showKeySchema: boolean;
    setShowKeySchema: (show: boolean) => void;
};

export default function SchemaHeader({
    maxVersion = 0,
    fetchVersions,
    editMode,
    setEditMode,
    hasRaw,
    showRaw,
    setShowRaw,
    hasKeySchema,
    showKeySchema,
    setShowKeySchema,
}: Props) {
    const onVersionChange = (version1, version2) => {
        if (version1 === null || version2 === null) {
            return;
        }
        fetchVersions?.(version1 - maxVersion, version2 - maxVersion);
    };

    return (
        <TabToolbar>
            <SchemaHeaderContainer>
                {maxVersion > 0 && !editMode && <CustomPagination onChange={onVersionChange} maxVersion={maxVersion} />}
                <div>
                    {hasRaw && (
                        <Button type="text" onClick={() => setShowRaw(!showRaw)}>
                            {showRaw ? (
                                <>
                                    <TableOutlined />
                                    <Typography.Text>Tabular</Typography.Text>
                                </>
                            ) : (
                                <>
                                    <FileTextOutlined />
                                    <Typography.Text>Raw</Typography.Text>
                                </>
                            )}
                        </Button>
                    )}
                    {hasKeySchema && (
                        <KeyValueButtonGroup>
                            <KeyButton $highlighted={showKeySchema} onClick={() => setShowKeySchema(true)}>
                                Key
                            </KeyButton>
                            <ValueButton $highlighted={!showKeySchema} onClick={() => setShowKeySchema(false)}>
                                Value
                            </ValueButton>
                        </KeyValueButtonGroup>
                    )}
                    {maxVersion > 0 &&
                        (editMode ? (
                            <ShowVersionButton onClick={() => setEditMode?.(false)}>Version History</ShowVersionButton>
                        ) : (
                            <ShowVersionButton onClick={() => setEditMode?.(true)}>Back</ShowVersionButton>
                        ))}
                </div>
            </SchemaHeaderContainer>
        </TabToolbar>
    );
}
