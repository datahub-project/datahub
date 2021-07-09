import React from 'react';
import { Button } from 'antd';
import styled from 'styled-components';
import CustomPagination from './CustomPagination';

const SchemaHeaderContainer = styled.div<{ edit?: string }>`
    display: flex;
    ${(props) => (props.edit ? 'justify-content: flex-end; padding-bottom: 16px;' : 'justify-content: space-between;')}
`;

const ShowVersionButton = styled(Button)`
    margin-right: 10px;
`;

type Props = {
    maxVersion: number;
    fetchVersions: (version1: number, version2: number) => void;
    editMode: boolean;
    setEditMode: (mode: boolean) => void;
    hasRow: boolean;
    showRaw: boolean;
    setShowRaw: (show: boolean) => void;
};

export default function SchemaHeader({
    maxVersion,
    fetchVersions,
    editMode,
    setEditMode,
    hasRow,
    showRaw,
    setShowRaw,
}: Props) {
    const onVersionChange = (version1, version2) => {
        if (version1 === null || version2 === null) {
            return;
        }
        fetchVersions(version1 - maxVersion, version2 - maxVersion);
    };

    return (
        <SchemaHeaderContainer edit={editMode ? 'true' : undefined}>
            {maxVersion > 0 && !editMode && <CustomPagination onChange={onVersionChange} maxVersion={maxVersion} />}
            <div>
                {maxVersion > 0 &&
                    (editMode ? (
                        <ShowVersionButton onClick={() => setEditMode(false)}>Version History</ShowVersionButton>
                    ) : (
                        <ShowVersionButton onClick={() => setEditMode(true)}>Back</ShowVersionButton>
                    ))}
                {hasRow && <Button onClick={() => setShowRaw(!showRaw)}>{showRaw ? 'Tabular' : 'Raw'}</Button>}
            </div>
        </SchemaHeaderContainer>
    );
}
