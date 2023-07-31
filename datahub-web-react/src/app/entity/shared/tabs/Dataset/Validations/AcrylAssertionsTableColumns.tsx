import React from 'react';
import styled from 'styled-components';
import { Tooltip, Typography, Dropdown, Button, Tag } from 'antd';
import { MoreOutlined, StopOutlined } from '@ant-design/icons';
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
} from '../../../../../../types.generated';
import { getResultColor, getResultIcon, getResultText } from './assertionUtils';
import { FreshnessAssertionDescription } from './FreshnessAssertionDescription';
import { InferredAssertionPopover } from './InferredAssertionPopover';
import { InferredAssertionBadge } from './InferredAssertionBadge';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../constants';
import { AssertionPlatformAvatar } from './AssertionPlatformAvatar';
import { AssertionActionsMenu } from './AssertionActionsMenu';

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

const UNKNOWN_DATA_PLATFORM = 'urn:li:dataPlatform:unknown';

interface DetailsColumnProps {
    assertion: Assertion;
    monitor?: Monitor;
    lastEvaluationTimeMs?: number;
    lastEvaluationResult?: AssertionResultType;
}

export function DetailsColumn({ assertion, monitor, lastEvaluationTimeMs, lastEvaluationResult }: DetailsColumnProps) {
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
            {(assertionType === AssertionType.Dataset && (
                <DatasetAssertionDescription assertionInfo={assertionInfo.datasetAssertion as DatasetAssertionInfo} />
            )) || (
                <FreshnessAssertionDescription
                    assertionInfo={assertionInfo.freshnessAssertion as FreshnessAssertionInfo}
                />
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
    lastEvaluationUrl?: string;
    onManageAssertion: () => void;
    onDeleteAssertion: () => void;
    onStartMonitor: () => void;
    onStopMonitor: () => void;
}

export function ActionsColumn({
    platform,
    monitor,
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
                <Tooltip title="Start running this assertion">
                    <StartMonitorButton type="primary" onClick={onStartMonitor}>
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
