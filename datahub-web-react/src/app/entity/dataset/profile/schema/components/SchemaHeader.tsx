import React from 'react';
import { Button, Pagination } from 'antd';
import styled from 'styled-components';

const SchemaHeaderContainer = styled.div<{ padding?: string }>`
    display: flex;
    justify-content: flex-end;
    ${(props) => (props.padding ? 'padding-bottom: 16px;' : '')}
`;

const ShowVersionButton = styled(Button)`
    margin-right: 10px;
`;
const PaginationContainer = styled(Pagination)`
    padding-top: 5px;
    margin-right: 15px;
`;

type Props = {
    currentVersion: number;
    setCurrentVersion: (currentVersion: number) => void;
    totalVersions: number;
    updateDiff: () => void;
    fetchVersions: (version1: number, version2: number) => void;
    editMode: boolean;
    setEditMode: (mode: boolean) => void;
    hasRow: boolean;
    showRaw: boolean;
    setShowRaw: (show: boolean) => void;
};

export default function SchemaHeader({
    currentVersion,
    setCurrentVersion,
    totalVersions,
    updateDiff,
    fetchVersions,
    editMode,
    setEditMode,
    hasRow,
    showRaw,
    setShowRaw,
}: Props) {
    const onVersionChange = (version) => {
        if (version === null) {
            return;
        }
        setCurrentVersion(version);
        if (version === totalVersions) {
            updateDiff();
            return;
        }
        fetchVersions(version - totalVersions, version - totalVersions - 1);
    };

    return (
        <SchemaHeaderContainer padding={editMode ? 'true' : undefined}>
            {totalVersions > 0 &&
                (editMode ? (
                    <ShowVersionButton onClick={() => setEditMode(false)}>Version History</ShowVersionButton>
                ) : (
                    <>
                        <ShowVersionButton
                            onClick={() => {
                                setEditMode(true);
                                setCurrentVersion(totalVersions);
                            }}
                        >
                            Back
                        </ShowVersionButton>
                        <PaginationContainer
                            simple
                            size="default"
                            defaultCurrent={totalVersions}
                            defaultPageSize={1}
                            total={totalVersions}
                            onChange={onVersionChange}
                            current={currentVersion}
                        />
                    </>
                ))}
            {hasRow && <Button onClick={() => setShowRaw(!showRaw)}>{showRaw ? 'Tabular' : 'Raw'}</Button>}
        </SchemaHeaderContainer>
    );
}
