import React from 'react';
import styled from 'styled-components';
import { Tooltip, Typography, Dropdown, Button, Tag } from 'antd';
import { ArrowRightOutlined, MoreOutlined, StopOutlined } from '@ant-design/icons';
import { DatasetAssertionDescription } from './DatasetAssertionDescription';
import {
    Assertion,
    AssertionType,
    DataPlatform,
    MonitorMode,
    AssertionSourceType,
    AssertionResultType,
    Monitor,
    DatasetAssertionInfo,
    FreshnessAssertionInfo,
    VolumeAssertionInfo,
    FieldAssertionInfo,
} from '../../../../../../types.generated';
import { getResultColor, getResultIcon, getResultText } from './assertionUtils';
import { FreshnessAssertionDescription } from './FreshnessAssertionDescription';
import { InferredAssertionPopover } from './InferredAssertionPopover';
import { InferredAssertionBadge } from './InferredAssertionBadge';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../constants';
import { AssertionPlatformAvatar } from './AssertionPlatformAvatar';
import { AssertionActionsMenu } from './AssertionActionsMenu';
import { VolumeAssertionDescription } from './VolumeAssertionDescription';
import { SqlAssertionDescription } from './SqlAssertionDescription';
import { FieldAssertionDescription } from './FieldAssertionDescription';

const DetailsContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
    &:hover {
        cursor: pointer;
    }
`;

const ResultTypeText = styled(Typography.Text)`
    margin-left: 8px;
`;

const ActionButtonContainer = styled.div`
    display: flex;
    justify-content: right;
    align-items: center;
`;

const StartMonitorButton = styled(Button)`
    letter-spacing: 2px;
    margin-right: 40px;
    background-color: ${REDESIGN_COLORS.BLUE};
`;

const StyledMoreOutlined = styled(MoreOutlined)`
    font-size: 18px;
`;

const StyledArrowRightOutlined = styled(ArrowRightOutlined)`
    font-size: 12px;
`;

const UNKNOWN_DATA_PLATFORM = 'urn:li:dataPlatform:unknown';

interface DetailsColumnProps {
    assertion: Assertion;
    monitor?: Monitor;
    lastEvaluationTimeMs?: number;
    lastEvaluationResult?: AssertionResultType;
    onViewAssertionDetails: () => void;
}

export function DetailsColumn({
    assertion,
    monitor,
    lastEvaluationTimeMs,
    lastEvaluationResult,
    onViewAssertionDetails,
}: DetailsColumnProps) {
    if (!assertion.info) {
        return <>No details found</>;
    }
    const assertionInfo = assertion.info;
    const assertionType = assertionInfo.type;
    const isInferred = assertionInfo.source?.type === AssertionSourceType.Inferred;
    const isStopped = monitor?.info?.status?.mode === MonitorMode.Inactive;
    const lastEvaluationDate = lastEvaluationTimeMs && new Date(lastEvaluationTimeMs);
    const lastEvaluationLocalString = lastEvaluationDate && `${lastEvaluationDate.toLocaleDateString()}`;
    const lastResultText =
        (!isStopped && ((lastEvaluationResult && getResultText(lastEvaluationResult)) || 'No Evaluations')) ||
        'Not running';
    const lastResultColor =
        (!isStopped && lastEvaluationResult && getResultColor(lastEvaluationResult)) || ANTD_GRAY[7];
    const lastResultIcon = (!isStopped && lastEvaluationResult && getResultIcon(lastEvaluationResult)) || (
        <StopOutlined />
    );
    return (
        <DetailsContainer>
            <Tooltip
                title={
                    (lastEvaluationLocalString && `Last evaluated on ${lastEvaluationLocalString}`) || 'No Evaluations'
                }
            >
                <Tag style={{ borderColor: lastResultColor }}>
                    {lastResultIcon}
                    <ResultTypeText style={{ color: lastResultColor }}>{lastResultText}</ResultTypeText>
                </Tag>
            </Tooltip>
            {assertionType === AssertionType.Dataset && (
                <DatasetAssertionDescription assertionInfo={assertionInfo.datasetAssertion as DatasetAssertionInfo} />
            )}
            {assertionType === AssertionType.Freshness && (
                <FreshnessAssertionDescription
                    assertionInfo={assertionInfo.freshnessAssertion as FreshnessAssertionInfo}
                    monitorSchedule={monitor?.info?.assertionMonitor?.assertions[0]?.schedule}
                />
            )}
            {assertionType === AssertionType.Volume && (
                <VolumeAssertionDescription assertionInfo={assertionInfo.volumeAssertion as VolumeAssertionInfo} />
            )}
            {assertionType === AssertionType.Sql && <SqlAssertionDescription assertionInfo={assertionInfo} />}
            {assertionType === AssertionType.Field && (
                <FieldAssertionDescription assertionInfo={assertionInfo.fieldAssertion as FieldAssertionInfo} />
            )}
            {[AssertionType.Freshness, AssertionType.Volume, AssertionType.Field, AssertionType.Sql].includes(
                assertionType,
            ) && (
                <Button
                    type="link"
                    onClick={(e) => {
                        e.stopPropagation();
                        onViewAssertionDetails();
                    }}
                >
                    Details <StyledArrowRightOutlined />
                </Button>
            )}
            {isInferred && (
                <InferredAssertionPopover>
                    <InferredAssertionBadge />
                </InferredAssertionPopover>
            )}
        </DetailsContainer>
    );
}

interface ActionsColumnProps {
    platform?: DataPlatform;
    monitor?: Monitor;
    canManageAssertion: boolean;
    lastEvaluationUrl?: string;
    onManageAssertion: () => void;
    onDeleteAssertion: () => void;
    onStartMonitor: () => void;
    onStopMonitor: () => void;
}

export function ActionsColumn({
    platform,
    monitor,
    canManageAssertion,
    lastEvaluationUrl,
    onManageAssertion,
    onDeleteAssertion,
    onStartMonitor,
    onStopMonitor,
}: ActionsColumnProps) {
    const isStopped = monitor?.info?.status?.mode === MonitorMode.Inactive;
    return (
        <ActionButtonContainer>
            {isStopped && (
                <Tooltip
                    title={
                        !canManageAssertion
                            ? 'A connection is required to run assertions. Configure your connection inside Ingestion, or contact your DataHub admin for help.'
                            : undefined
                    }
                >
                    <StartMonitorButton type="primary" onClick={onStartMonitor} disabled={!canManageAssertion}>
                        TURN ON
                    </StartMonitorButton>
                </Tooltip>
            )}
            {platform && platform.urn !== UNKNOWN_DATA_PLATFORM && (
                <AssertionPlatformAvatar platform={platform} lastEvaluationUrl={lastEvaluationUrl} />
            )}
            <Dropdown
                overlay={
                    <AssertionActionsMenu
                        monitor={monitor}
                        canManageAssertion={canManageAssertion}
                        onManageAssertion={onManageAssertion}
                        onDeleteAssertion={onDeleteAssertion}
                        onStartMonitor={onStartMonitor}
                        onStopMonitor={onStopMonitor}
                    />
                }
                trigger={['click']}
            >
                <StyledMoreOutlined />
            </Dropdown>
        </ActionButtonContainer>
    );
}
