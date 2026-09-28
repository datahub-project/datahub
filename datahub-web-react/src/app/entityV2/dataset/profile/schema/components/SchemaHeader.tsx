import { Button, Icon, TabButtonItem, TabButtons, Tooltip } from '@components';
import { ClockCounterClockwise } from '@phosphor-icons/react/dist/csr/ClockCounterClockwise';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { Table } from '@phosphor-icons/react/dist/csr/Table';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useDebounce } from 'react-use';
import styled from 'styled-components/macro';

import SchemaSearchInput from '@app/entityV2/dataset/profile/schema/components/SchemaSearchInput';
import VersionSelector from '@app/entityV2/dataset/profile/schema/components/VersionSelector';
import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import AddLogicalModelColumnButton from '@app/entityV2/shared/logicalModels/AddLogicalModelColumnButton';
import { SchemaFilterType } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';

import { SemanticVersionStruct } from '@types';

const SCHEMA_VIEW_TABULAR = 'tabular';
const SCHEMA_VIEW_RAW = 'raw';

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
    gap: 8px;

    padding-left: 5px;
`;

const SchemaViewSwitch = styled.div`
    flex-shrink: 0;
`;

const SchemaViewIcon = styled.span`
    display: inline-flex;
    align-items: center;
    justify-content: center;
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
    setShowSchemaTimeline: (show: boolean) => void;
    filterText: string;
    setFilterText: (text: string) => void;
    numRows: number;
    schemaFilterTypes: SchemaFilterType[];
    setSchemaFilterTypes: (filters: SchemaFilterType[]) => void;
    highlightedMatchIndex: number | null;
    setHighlightedMatchIndex: (val: number | null) => void;
    matches: { path: string; index: number }[];
    showAddLogicalModelColumnButton?: boolean;
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
    showAddLogicalModelColumnButton,
}: Props) {
    const { t } = useTranslation('entity.types');
    const [schemaFilterSelectOpen, setSchemaFilterSelectOpen] = useState(false);

    const schemaAuditToggleText = showSchemaTimeline ? t('dataset.closeChangeHistory') : t('dataset.viewChangeHistory');
    const schemaViewActiveKey = showRaw ? SCHEMA_VIEW_RAW : SCHEMA_VIEW_TABULAR;

    const schemaViewTabs: TabButtonItem[] = [
        {
            key: SCHEMA_VIEW_TABULAR,
            label: (
                <Tooltip title={t('dataset.tabularView')} showArrow={false}>
                    <SchemaViewIcon>
                        <Icon icon={Table} size="lg" color="inherit" />
                    </SchemaViewIcon>
                </Tooltip>
            ),
            dataTestId: 'schema-tabular-view-button',
        },
        {
            key: SCHEMA_VIEW_RAW,
            label: (
                <Tooltip title={t('dataset.rawView')} showArrow={false}>
                    <SchemaViewIcon>
                        <Icon icon={FileText} size="lg" color="inherit" />
                    </SchemaViewIcon>
                </Tooltip>
            ),
            dataTestId: 'schema-raw-view-button',
        },
    ];

    const [searchInput, setSearchInput] = useState(filterText);
    useDebounce(() => setFilterText(searchInput), 100, [searchInput]);

    return (
        <StyledTabToolbar>
            <SchemaHeaderContainer>
                <LeftButtonsGroup>
                    {hasKeySchema && (
                        <KeyValueButtonGroup>
                            <KeyButton $highlighted={showKeySchema} onClick={() => setShowKeySchema(true)}>
                                {t('dataset.keyToggle')}
                            </KeyButton>
                            <ValueButton $highlighted={!showKeySchema} onClick={() => setShowKeySchema(false)}>
                                {t('dataset.valueToggle')}
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
                    {showAddLogicalModelColumnButton && <AddLogicalModelColumnButton />}
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
                    {hasRaw && (
                        <SchemaViewSwitch>
                            <TabButtons
                                tabs={schemaViewTabs}
                                activeTab={schemaViewActiveKey}
                                onTabClick={(key) => setShowRaw(key === SCHEMA_VIEW_RAW)}
                                fit="hug"
                            />
                        </SchemaViewSwitch>
                    )}
                    <Tooltip title={schemaAuditToggleText} showArrow={false}>
                        <Button
                            variant="text"
                            data-testid="schema-blame-button"
                            color={showSchemaTimeline ? 'violet' : 'gray'}
                            icon={{ icon: ClockCounterClockwise, size: '2xl' }}
                            onClick={() => setShowSchemaTimeline(!showSchemaTimeline)}
                        />
                    </Tooltip>
                </RightButtonsGroup>
            </SchemaHeaderContainer>
        </StyledTabToolbar>
    );
}
