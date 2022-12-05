import React from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import { Button, Input, Popover, Select, Tooltip, Typography } from 'antd';
import { debounce } from 'lodash';
import {
    AuditOutlined,
    CaretDownOutlined,
    FileTextOutlined,
    QuestionCircleOutlined,
    SearchOutlined,
    TableOutlined,
} from '@ant-design/icons';
import styled from 'styled-components/macro';
import CustomPagination from './CustomPagination';
import TabToolbar from '../../../../shared/components/styled/TabToolbar';
import { SemanticVersionStruct } from '../../../../../../types.generated';
import { toRelativeTimeString } from '../../../../../shared/time/timeUtils';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../../shared/constants';
import { navigateToVersionedDatasetUrl } from '../../../../shared/tabs/Dataset/Schema/utils/navigateToVersionedDatasetUrl';
import SchemaTimeStamps from './SchemaTimeStamps';
import getSchemaFilterFromQueryString from '../../../../shared/tabs/Dataset/Schema/utils/getSchemaFilterFromQueryString';

const SchemaHeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
`;

// TODO(Gabe): undo display: none when dbt/bigquery flickering has been resolved
const ShowVersionButton = styled(Button)`
    display: inline-block;
    margin-right: 10px;
    display: none;
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
    padding-left: 5px;
    &&& {
        display: flex;
        justify-content: right;
        margin-top: -6px;
        width: 100%;
        row-gap: 12px;
    }
`;

const SchemaBlameSelector = styled(Select)`
    &&& {
        font-weight: 400;
        margin-top: 6px;
        min-width: 30px;
        margin-right: 10px;
        border-radius: 0px 8px 8px 0px;
    }
`;

const SchemaBlameSelectorOption = styled(Select.Option)`
    &&& {
        overflow: visible;
        margin-top: 6px;
    }
`;

const SchemaAuditButton = styled(Button)`
    &&& {
        margin-top: 6px;
    }
`;

const StyledQuestionCircleOutlined = styled(QuestionCircleOutlined)`
    &&& {
        margin-top: 14px;
        font-size: 16px;
        color: ${ANTD_GRAY[6]};
    }
`;

const StyledInput = styled(Input)`
    border-radius: 70px;
    max-width: 300px;
`;

const MAX_ROWS_BEFORE_DEBOUNCE = 50;
const HALF_SECOND_IN_MS = 500;

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
    lastUpdated?: number | null;
    lastObserved?: number | null;
    selectedVersion: string;
    versionList: Array<SemanticVersionStruct>;
    showSchemaAuditView: boolean;
    setShowSchemaAuditView: any;
    setFilterText: (text: string) => void;
    numRows: number;
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
    lastUpdated,
    lastObserved,
    selectedVersion,
    versionList,
    showSchemaAuditView,
    setShowSchemaAuditView,
    setFilterText,
    numRows,
}: Props) {
    const history = useHistory();
    const location = useLocation();
    const onVersionChange = (version1, version2) => {
        if (version1 === null || version2 === null) {
            return;
        }
        fetchVersions?.(version1 - maxVersion, version2 - maxVersion);
    };

    const semanticVersionDisplayString = (semanticVersion: SemanticVersionStruct) => {
        const semanticVersionTimestampString =
            (semanticVersion?.semanticVersionTimestamp &&
                toRelativeTimeString(semanticVersion?.semanticVersionTimestamp)) ||
            'unknown';
        return `${semanticVersion.semanticVersion} - ${semanticVersionTimestampString}`;
    };
    const numVersions = versionList.length;

    const renderOptions = () => {
        return versionList.map(
            (semanticVersionStruct) =>
                semanticVersionStruct?.semanticVersion &&
                semanticVersionStruct?.semanticVersionTimestamp && (
                    <SchemaBlameSelectorOption
                        value={semanticVersionStruct?.semanticVersion}
                        data-testid={`sem-ver-select-button-${semanticVersionStruct?.semanticVersion}`}
                    >
                        {semanticVersionDisplayString(semanticVersionStruct)}
                    </SchemaBlameSelectorOption>
                ),
        );
    };
    const schemaAuditToggleText = showSchemaAuditView ? 'Close column history' : 'View column history';

    const debouncedSetFilterText = debounce(
        (e: React.ChangeEvent<HTMLInputElement>) => setFilterText(e.target.value),
        numRows > MAX_ROWS_BEFORE_DEBOUNCE ? HALF_SECOND_IN_MS : 0,
    );
    const schemaFilter = getSchemaFilterFromQueryString(location);

    const docLink = 'https://datahubproject.io/docs/dev-guides/timeline/';
    return (
        <TabToolbar>
            <SchemaHeaderContainer>
                {maxVersion > 0 && !editMode && <CustomPagination onChange={onVersionChange} maxVersion={maxVersion} />}
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
                    {maxVersion > 0 &&
                        (editMode ? (
                            <ShowVersionButton onClick={() => setEditMode?.(false)}>Version Blame</ShowVersionButton>
                        ) : (
                            <ShowVersionButton onClick={() => setEditMode?.(true)}>Back</ShowVersionButton>
                        ))}
                    {!showRaw && (
                        <StyledInput
                            defaultValue={schemaFilter}
                            placeholder="Search in schema..."
                            onChange={debouncedSetFilterText}
                            allowClear
                            prefix={<SearchOutlined />}
                        />
                    )}
                </LeftButtonsGroup>
                <RightButtonsGroup>
                    <SchemaTimeStamps lastObserved={lastObserved} lastUpdated={lastUpdated} />
                    <Tooltip title={schemaAuditToggleText}>
                        <SchemaAuditButton
                            type="text"
                            data-testid="schema-blame-button"
                            onClick={() => setShowSchemaAuditView(!showSchemaAuditView)}
                            style={{ color: showSchemaAuditView ? REDESIGN_COLORS.BLUE : ANTD_GRAY[7] }}
                        >
                            <AuditOutlined />
                        </SchemaAuditButton>
                    </Tooltip>
                    {numVersions > 1 && (
                        <>
                            <SchemaBlameSelector
                                value={selectedVersion}
                                onChange={(e) => {
                                    const datasetVersion: string = e as string;
                                    navigateToVersionedDatasetUrl({
                                        location,
                                        history,
                                        datasetVersion,
                                    });
                                }}
                                data-testid="schema-version-selector-dropdown"
                                suffixIcon={<CaretDownOutlined />}
                            >
                                {renderOptions()}
                            </SchemaBlameSelector>
                            <Popover
                                overlayStyle={{ maxWidth: 240 }}
                                placement="right"
                                content={
                                    <div>
                                        Semantic versions for this view were computed using Technical Schema. You can
                                        find more info about how DataHub computes versions
                                        <a target="_blank" rel="noreferrer noopener" href={docLink}>
                                            {' '}
                                            here.{' '}
                                        </a>
                                    </div>
                                }
                            >
                                <StyledQuestionCircleOutlined />
                            </Popover>
                        </>
                    )}
                </RightButtonsGroup>
            </SchemaHeaderContainer>
        </TabToolbar>
    );
}
