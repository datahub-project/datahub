import React from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import { Button, Popover, Radio, Select, Typography } from 'antd';
import { CaretDownOutlined, FileTextOutlined, InfoCircleOutlined, TableOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import CustomPagination from './CustomPagination';
import TabToolbar from '../../../../shared/components/styled/TabToolbar';
import { SemanticVersionStruct } from '../../../../../../types.generated';
import { toRelativeTimeString } from '../../../../../shared/time/timeUtils';
import { SchemaViewType } from '../utils/types';
import { ANTD_GRAY } from '../../../../shared/constants';
import { navigateToVersionedDatasetUrl } from '../../../../shared/tabs/Dataset/Schema/utils/navigateToVersionedDatasetUrl';

const SchemaHeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    padding-bottom: 16px;
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
    display: inline-block;
`;

// Below styles are for buttons on the right side of the Schema Header
const RightButtonsGroup = styled.div`
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
        min-width: 17%;
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

const BlameRadio = styled(Radio.Group)`
    &&& {
        margin-top: 6px;
        margin-right: 10px;
    }
`;

const BlameRadioButton = styled(Radio.Button)`
    &&& {
        min-width: 30px;
    }
`;

const CurrentVersionTimestampText = styled(Typography.Text)`
    &&& {
        line-height: 22px;
        margin-top: 10px;
        margin-right: 10px;
        color: ${ANTD_GRAY[7]};
    }
`;

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)`
    &&& {
        margin-top: 12px;
        font-size: 20px;
        color: ${ANTD_GRAY[6]};
    }
`;

const StyledCaretDownOutlined = styled(CaretDownOutlined)`
    &&& {
        margin-top: 8px;
    }
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
    lastUpdatedTimeString: string;
    selectedVersion: string;
    versionList: Array<SemanticVersionStruct>;
    schemaView: SchemaViewType;
    setSchemaView: any;
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
    lastUpdatedTimeString,
    selectedVersion,
    versionList,
    schemaView,
    setSchemaView,
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

    const onSchemaViewToggle = (e) => {
        setSchemaView(e.target.value);
    };

    const docLink = 'https://datahubproject.io/docs/dev-guides/timeline/';
    return (
        <TabToolbar>
            <SchemaHeaderContainer>
                {maxVersion > 0 && !editMode && <CustomPagination onChange={onVersionChange} maxVersion={maxVersion} />}
                <LeftButtonsGroup>
                    {hasRaw && (
                        <RawButton type="text" onClick={() => setShowRaw(!showRaw)}>
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
                </LeftButtonsGroup>
                <RightButtonsGroup>
                    <CurrentVersionTimestampText>{lastUpdatedTimeString}</CurrentVersionTimestampText>
                    <BlameRadio value={schemaView} onChange={onSchemaViewToggle}>
                        <BlameRadioButton value={SchemaViewType.NORMAL} data-testid="schema-normal-button">
                            Normal
                        </BlameRadioButton>
                        <BlameRadioButton value={SchemaViewType.BLAME} data-testid="schema-blame-button">
                            Blame
                        </BlameRadioButton>
                    </BlameRadio>
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
                        suffixIcon={<StyledCaretDownOutlined />}
                    >
                        {renderOptions()}
                    </SchemaBlameSelector>
                    <Popover
                        overlayStyle={{ maxWidth: 240 }}
                        placement="right"
                        content={
                            <div>
                                Semantic versions for this view were computed using Technical Schema. You can find more
                                info about how we compute versions
                                <a target="_blank" rel="noreferrer noopener" href={docLink}>
                                    {' '}
                                    here.{' '}
                                </a>
                            </div>
                        }
                    >
                        <StyledInfoCircleOutlined />
                    </Popover>
                </RightButtonsGroup>
            </SchemaHeaderContainer>
        </TabToolbar>
    );
}
