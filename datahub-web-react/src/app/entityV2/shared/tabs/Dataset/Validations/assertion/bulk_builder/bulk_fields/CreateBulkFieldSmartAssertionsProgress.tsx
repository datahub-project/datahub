import { CheckCircleFilled, ExclamationCircleFilled } from '@ant-design/icons';
import { Button } from '@components';
import { Collapse, Progress, Tooltip, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { ProgressReport } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/bulk_builder/bulk_fields/useBulkCreateFieldAssertions';
import { getFieldMetricTypeReadableLabel } from '@app/entityV2/shared/tabs/Dataset/Validations/fieldDescriptionUtils';

import { FieldMetricType, SchemaFieldSpecInput } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding: 16px;
`;

const ProgressContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

const StatusRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const StatusItem = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 14px;
`;

const SuccessIcon = styled(CheckCircleFilled)`
    color: ${REDESIGN_COLORS.GREEN_NORMAL};
`;

const ErrorIcon = styled(ExclamationCircleFilled)`
    color: ${REDESIGN_COLORS.RED_ERROR};
`;

const ProgressText = styled(Typography.Text)`
    font-size: 16px;
    font-weight: 600;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;

const StyledProgress = styled(Progress)`
    .ant-progress-bg {
        background: linear-gradient(90deg, ${REDESIGN_COLORS.GREEN_NORMAL} 0%, ${REDESIGN_COLORS.TERTIARY_GREEN} 100%);
    }
`;

const SuccessfulContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const SuccessfulSummary = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
`;

const SuccessfulPill = styled.div`
    background: ${REDESIGN_COLORS.GREEN_LIGHT};
    color: ${REDESIGN_COLORS.GREEN_800};
    padding: 4px 8px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 500;
`;

const MorePill = styled.div`
    background: ${REDESIGN_COLORS.GREY_100};
    color: ${REDESIGN_COLORS.BODY_TEXT};
    padding: 4px 8px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 500;
    cursor: pointer;
    transition: background-color 0.2s;

    &:hover {
        background: ${REDESIGN_COLORS.PRIMARY_PURPLE};
        color: white;
    }
`;

const ErrorContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const StyledCollapse = styled(Collapse)`
    background: ${REDESIGN_COLORS.YELLOW_200};
    border: 1px solid ${REDESIGN_COLORS.YELLOW_600};
    border-radius: 8px;

    .ant-collapse-header {
        background: ${REDESIGN_COLORS.YELLOW_200};
        padding: 12px 16px !important;
        border-radius: 8px;
        font-weight: 600;
        color: ${REDESIGN_COLORS.WARNING_YELLOW};
    }

    .ant-collapse-content-box {
        padding: 16px;
        background: white;
        border-top: 1px solid ${REDESIGN_COLORS.YELLOW_600};
    }

    .ant-collapse-item {
        border: none;
        margin-bottom: 8px;

        &:last-child {
            margin-bottom: 0;
        }
    }
`;

const ErrorMessage = styled.div`
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    font-size: 14px;
    line-height: 1.5;
    word-break: break-word;
`;

const TooltipContent = styled.div`
    max-width: 300px;
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const TooltipItem = styled.div`
    font-size: 12px;
    padding: 2px 0;
`;

const CompletedMessage = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 12px;
    background: ${REDESIGN_COLORS.GREEN_LIGHT};
    border: 1px solid ${REDESIGN_COLORS.GREEN_NORMAL};
    border-radius: 8px;
    font-weight: 600;
    color: ${REDESIGN_COLORS.GREEN_800};
`;

type Props = {
    progress: ProgressReport;
    onDone: () => void;
};

export const CreateBulkFieldSmartAssertionsProgress = ({ progress, onDone }: Props) => {
    const { total, completed, successful, errored } = progress;

    const progressPercentage = total > 0 ? (completed / total) * 100 : 0;
    const isCompleted = completed === total && total > 0;

    const getFieldPath = (field: SchemaFieldSpecInput | 'Unknown'): string => {
        if (field === 'Unknown') return 'Unknown';
        return field.path || 'Unknown';
    };

    const getMetricLabel = (metric: FieldMetricType | 'Unknown'): string => {
        if (metric === 'Unknown') return 'Unknown';
        try {
            return getFieldMetricTypeReadableLabel(metric);
        } catch {
            return String(metric);
        }
    };

    const renderSuccessfulSummary = () => {
        if (successful.length === 0) return null;

        const maxVisible = 3;
        const visibleSuccessful = successful.slice(0, maxVisible);
        const remainingCount = successful.length - maxVisible;

        const tooltipContent = (
            <TooltipContent>
                {successful.map((item) => (
                    <TooltipItem key={`${getFieldPath(item.field)}-${getMetricLabel(item.metric)}`}>
                        {getFieldPath(item.field)} – {getMetricLabel(item.metric)}
                    </TooltipItem>
                ))}
            </TooltipContent>
        );

        return (
            <SuccessfulContainer>
                <Typography.Text strong style={{ color: REDESIGN_COLORS.GREEN_800 }}>
                    Successfully Created ({successful.length})
                </Typography.Text>
                <SuccessfulSummary>
                    {visibleSuccessful.map((item) => (
                        <SuccessfulPill key={`${getFieldPath(item.field)}-${getMetricLabel(item.metric)}`}>
                            {getFieldPath(item.field)} – {getMetricLabel(item.metric)}
                        </SuccessfulPill>
                    ))}
                    {remainingCount > 0 && (
                        <Tooltip title={tooltipContent} placement="bottom" overlayStyle={{ maxWidth: 400 }}>
                            <MorePill>+{remainingCount} more</MorePill>
                        </Tooltip>
                    )}
                </SuccessfulSummary>
            </SuccessfulContainer>
        );
    };

    const renderErrors = () => {
        if (errored.length === 0) return null;

        return (
            <ErrorContainer>
                <Typography.Text strong style={{ color: REDESIGN_COLORS.WARNING_YELLOW }}>
                    Errors ({errored.length})
                </Typography.Text>
                <StyledCollapse ghost>
                    {errored.map((error) => (
                        <Collapse.Panel
                            key={`${getFieldPath(error.field)}-${getMetricLabel(error.metric)}`}
                            header={
                                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                    <ErrorIcon />
                                    <strong>{getFieldPath(error.field)}</strong> {getMetricLabel(error.metric)}
                                </div>
                            }
                        >
                            <ErrorMessage>{error.error}</ErrorMessage>
                        </Collapse.Panel>
                    ))}
                </StyledCollapse>
            </ErrorContainer>
        );
    };

    return (
        <Container>
            <ProgressContainer>
                <StatusRow>
                    <ProgressText>
                        {isCompleted
                            ? 'Bulk Assertion Creation Complete'
                            : `Creating Assertions... ${completed}/${total}`}
                    </ProgressText>
                    <div style={{ display: 'flex', gap: 16 }}>
                        <StatusItem>
                            <SuccessIcon />
                            <span>{successful.length} Successful</span>
                        </StatusItem>
                        {errored.length > 0 && (
                            <StatusItem>
                                <ErrorIcon />
                                <span>{errored.length} Errors</span>
                            </StatusItem>
                        )}
                    </div>
                </StatusRow>

                <StyledProgress percent={progressPercentage} showInfo={false} strokeWidth={8} />
            </ProgressContainer>

            {isCompleted && (
                <CompletedMessage>
                    <CheckCircleFilled />
                    All assertions have been processed. {successful.length} successful, {errored.length} errors.
                </CompletedMessage>
            )}

            {renderSuccessfulSummary()}
            {renderErrors()}

            {isCompleted && (
                <Button onClick={onDone} style={{ width: 'fit-content', alignSelf: 'flex-end' }}>
                    Done
                </Button>
            )}
        </Container>
    );
};
