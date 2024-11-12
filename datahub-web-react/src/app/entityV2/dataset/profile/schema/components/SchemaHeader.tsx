import { FileTextOutlined, TableOutlined } from '@ant-design/icons';
import VersionSelector from '@app/entityV2/dataset/profile/schema/components/VersionSelector';
import HistoryIcon from '@mui/icons-material/History';
import { Button, Typography } from 'antd';
import { Tooltip } from '@components';
import { debounce } from 'lodash';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { SemanticVersionStruct } from '../../../../../../types.generated';
import TabToolbar from '../../../../shared/components/styled/TabToolbar';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../../shared/constants';
import { SchemaFilterType } from '../../../../shared/tabs/Dataset/Schema/utils/filterSchemaRows';
import SchemaSearchInput from './SchemaSearchInput';

const SchemaHeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
    padding-bottom: 3px;
`;

// Below styles are for buttons on the left side of the Schema Header
const LeftButtonsGroup = styled.div`
    &&& {
        display: flex;
        justify-content: left;
        width: 100%;
    }
`;

const RawButton = styled(Button)`
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

const SchemaAuditButton = styled(Button)`
    display: flex;
    align-items: center;
    background: ${REDESIGN_COLORS.WHITE};
    padding: 0;
    margin-right: 15px;

    svg {
        background: ${REDESIGN_COLORS.TITLE_PURPLE};
        border-radius: 50%;
        stroke: ${REDESIGN_COLORS.WHITE};
        color: ${REDESIGN_COLORS.WHITE};
        padding: 4px;
        stroke-width: 0.5px;
    }
`;

const MAX_ROWS_BEFORE_DEBOUNCE = 50;

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
    setFilterText: (text: string) => void;
    numRows: number;
    schemaFilterTypes: SchemaFilterType[];
    setSchemaFilterTypes: (filters: SchemaFilterType[]) => void;
    highlightedMatchIndex: number | null;
    setHighlightedMatchIndex: (val: number | null) => void;
    matches: { path: string; index: number }[];
    schemaFilter: string;
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
    setFilterText,
    numRows,
    schemaFilterTypes,
    setSchemaFilterTypes,
    matches,
    highlightedMatchIndex,
    setHighlightedMatchIndex,
    schemaFilter,
}: Props) {
    const [schemaFilterSelectOpen, setSchemaFilterSelectOpen] = useState(false);

    const schemaAuditToggleText = showSchemaTimeline ? 'Close change history' : 'View change history';

    const debouncedSetFilterText = debounce(
        (e: React.ChangeEvent<HTMLInputElement>) => setFilterText(e.target.value),
        numRows > MAX_ROWS_BEFORE_DEBOUNCE ? 100 : 0,
    );

    return (
        <TabToolbar>
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
                            schemaFilter={schemaFilter}
                            debouncedSetFilterText={debouncedSetFilterText}
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
                        <SchemaAuditButton
                            type="text"
                            data-testid="schema-blame-button"
                            onClick={() => setShowSchemaTimeline(!showSchemaTimeline)}
                            style={{ color: showSchemaTimeline ? REDESIGN_COLORS.BLUE : ANTD_GRAY[7] }}
                        >
                            <HistoryIcon />
                        </SchemaAuditButton>
                    </Tooltip>
                </RightButtonsGroup>
            </SchemaHeaderContainer>
        </TabToolbar>
    );
}
