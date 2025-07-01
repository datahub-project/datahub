import { FileTextOutlined, TableOutlined } from '@ant-design/icons';
import { Button, Tooltip } from '@components';
import { Button as AntButton, Typography } from 'antd';
import React, { useState } from 'react';
import { useDebounce } from 'react-use';
import styled from 'styled-components/macro';

import SchemaSearchInput from '@app/entityV2/dataset/profile/schema/components/SchemaSearchInput';
import VersionSelector from '@app/entityV2/dataset/profile/schema/components/VersionSelector';
import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import { SchemaFilterType } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';

import { SemanticVersionStruct } from '@types';

const StyledTabToolbar = styled(TabToolbar)`
    height: unset;
    padding: 8px 16px 16px 16px;
`;

const SchemaHeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
`;

// Below styles are for buttons on the left side of the Schema Header
const LeftButtonsGroup = styled.div`
    &&& {
        display: flex;
        justify-content: left;
        width: 100%;
    }
`;

const RawButton = styled(AntButton)`
    &&& {
        display: flex;
        margin-right: 10px;
        justify-content: left;
        align-items: center;
    }
`;

const RawButtonTitleContainer = styled.span`
    display: flex;
    align-items: center;
`;

const RawButtonTitle = styled(Typography.Text)`
    margin-left: 6px;
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
    display: flex;
`;

// Below styles are for buttons on the right side of the Schema Header
const RightButtonsGroup = styled.div`
    display: flex;
    align-items: center;
    justify-content: right;
    gap: 15px;

    padding-left: 5px;
`;

type Props = {
    hasRaw: boolean;
    showRaw: boolean;
    setShowRaw: (show: boolean) => void;
    hasKeySchema: boolean;
    showKeySchema: boolean;
    setShowKeySchema: (show: boolean) => void;
    selectedVersion: string;
    versionList: Array<SemanticVersionStruct>;
    showSchemaTimeline: boolean;
    setShowSchemaTimeline: any;
    filterText: string;
    setFilterText: (text: string) => void;
    numRows: number;
    schemaFilterTypes: SchemaFilterType[];
    setSchemaFilterTypes: (filters: SchemaFilterType[]) => void;
    highlightedMatchIndex: number | null;
    setHighlightedMatchIndex: (val: number | null) => void;
    matches: { path: string; index: number }[];
};

export default function SchemaHeader({
    hasRaw,
    showRaw,
    setShowRaw,
    hasKeySchema,
    showKeySchema,
    setShowKeySchema,
    selectedVersion,
    versionList,
    setShowSchemaTimeline,
    showSchemaTimeline,
    filterText,
    setFilterText,
    numRows,
    schemaFilterTypes,
    setSchemaFilterTypes,
    matches,
    highlightedMatchIndex,
    setHighlightedMatchIndex,
}: Props) {
    const [schemaFilterSelectOpen, setSchemaFilterSelectOpen] = useState(false);

    const schemaAuditToggleText = showSchemaTimeline ? 'Close change history' : 'View change history';

    const [searchInput, setSearchInput] = useState(filterText);
    useDebounce(() => setFilterText(searchInput), 100, [searchInput]);

    return (
        <StyledTabToolbar>
            <SchemaHeaderContainer>
                <LeftButtonsGroup>
                    {hasRaw && (
                        <RawButton type="text" onClick={() => setShowRaw(!showRaw)}>
                            {showRaw ? (
                                <RawButtonTitleContainer>
                                    <TableOutlined style={{ padding: 0, margin: 0 }} />
                                    <RawButtonTitle>Tabular</RawButtonTitle>
                                </RawButtonTitleContainer>
                            ) : (
                                <RawButtonTitleContainer>
                                    <FileTextOutlined style={{ padding: 0, margin: 0 }} />
                                    <RawButtonTitle>Raw</RawButtonTitle>
                                </RawButtonTitleContainer>
                            )}
                        </RawButton>
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
                    {!showRaw && (
                        <SchemaSearchInput
                            schemaFilterTypes={schemaFilterTypes}
                            setSchemaFilterTypes={setSchemaFilterTypes}
                            searchInput={searchInput}
                            setSearchInput={setSearchInput}
                            matches={matches.map((match) => match.path)}
                            highlightedMatchIndex={highlightedMatchIndex}
                            setHighlightedMatchIndex={setHighlightedMatchIndex}
                            schemaFilterSelectOpen={schemaFilterSelectOpen}
                            setSchemaFilterSelectOpen={setSchemaFilterSelectOpen}
                            numRows={numRows}
                        />
                    )}
                </LeftButtonsGroup>
                <RightButtonsGroup>
                    {versionList.length > 1 && (
                        <VersionSelector
                            versionList={versionList}
                            selectedVersion={selectedVersion}
                            isSibling={false}
                            isPrimary
                        />
                    )}
                    <Tooltip title={schemaAuditToggleText} showArrow={false}>
                        <Button
                            variant="text"
                            data-testid="schema-blame-button"
                            color={showSchemaTimeline ? 'violet' : 'gray'}
                            icon={{ icon: 'ClockCounterClockwise', source: 'phosphor', size: '2xl' }}
                            onClick={() => setShowSchemaTimeline(!showSchemaTimeline)}
                        />
                    </Tooltip>
                </RightButtonsGroup>
            </SchemaHeaderContainer>
        </StyledTabToolbar>
    );
}
