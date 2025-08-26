import { Radio, RadioChangeEvent, Typography } from 'antd';
import React, { useRef } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import {
    DATE_COMMA_TIME_TZ,
    formatTimestamp,
} from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/utils';
import { useQualityTabContext } from '@app/entityV2/shared/tabs/Dataset/Validations/QualityTabContextProvider';
import { DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/constants';
import { FixedIntervalScheduleBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/freshness/FixedIntervalSchedulerBuilder';
import {
    computeAverageUpdateFrequencyInMillis,
    getFormattedTimeStringTimeSince,
    mostRecentOperationsTimeSinceInMillis,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/freshness/utils';
import { AssertionFormTitleAndTooltip } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/AssertionFormTitleAndTooltip';
import {
    FreshnessAssertionBuilderSchedule,
    FreshnessAssertionScheduleBuilderTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { formatDuration } from '@app/shared/formatDuration';
import { Tooltip, colors } from '@src/alchemy-components';
import { useAppConfig } from '@src/app/useAppConfig';

import { useGetOperationsQuery } from '@graphql/dataset.generated';
import { FreshnessAssertionScheduleType } from '@types';

const MAX_LOOKBACK_WINDOW_DAYS = 30;
const DAYS_TO_MILLISECONDS = 1000 * 60 * 60 * 24;
const MAX_LOOKBACK_WINDOW = MAX_LOOKBACK_WINDOW_DAYS * DAYS_TO_MILLISECONDS;
const MAX_LOOKBACK_OPERATIONS = 100;
export const MAX_NUM_LATEST_UPDATES_SHOWN = 3;

const Form = styled.div`
    margin: 16px 0 24px;
`;

const Title = styled(Typography.Title)`
    && {
        margin-bottom: 12px;
    }
`;

const RadioGroup = styled(Radio.Group)`
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-bottom: 8px;
`;

const RadioContainer = styled.div`
    background-color: ${ANTD_GRAY[1]};
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 12px;
    padding: 8px 16px;
    height: 100px;
    display: flex;
    align-items: center;
`;

const StyledRadio = styled(Radio)`
    display: flex;
    align-items: center;
`;

const TextContainer = styled.div`
    margin-left: 4px;
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const RecentUpdatesSection = styled.div`
    margin-bottom: 16px;
    padding: 16px;
    border: 1px solid ${colors.gray[1000]};
    background-color: ${colors.gray[1500]};
    border-radius: 12px;

    .assertion-form-title {
        font-size: 14px;
        font-weight: 600;
    }
`;

const UpdateOperationsSummary = styled.div`
    margin-top: 4px;
`;

type Props = {
    value?: FreshnessAssertionBuilderSchedule;
    onChange: (schedule: FreshnessAssertionBuilderSchedule) => void;
    disabled?: boolean;
    isEditMode?: boolean;
};

export const DatasetFreshnessScheduleBuilder = ({ value, onChange, disabled, isEditMode }: Props) => {
    const { type: scheduleType, fixedInterval } = value ?? {};

    const { onlineSmartAssertionsEnabled } = useAppConfig().config.featureFlags;
    const isDetectWithAIEnabled = !isEditMode && onlineSmartAssertionsEnabled;

    const handleScheduleTypeChange = (newScheduleType: FreshnessAssertionScheduleType) => {
        onChange({
            ...value,
            type: newScheduleType,
            // If we are switching to fixed interval assertion, be sure to set a default interval!
            fixedInterval: value?.fixedInterval ?? DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE.schedule?.fixedInterval,
        });
    };

    const handleFixedIntervalScheduleChange = (
        newFixedSchedule: FreshnessAssertionBuilderSchedule['fixedInterval'],
    ) => {
        onChange({
            ...value,
            fixedInterval: newFixedSchedule,
        });
    };

    const { qualityEntityUrn, canViewDatasetProfile: canViewDatasetProfileFromQualityTabContext } =
        useQualityTabContext();

    const nowMs = useRef(Date.now()).current; // useRef to avoid re-rendering on every render
    const {
        data: operationsData,
        loading: operationsLoading,
        error: operationsError,
    } = useGetOperationsQuery({
        fetchPolicy: 'cache-first',
        variables: {
            urn: qualityEntityUrn ?? '',
            // Fetch the most recent operations for the dataset within the lookback window:
            startTime: nowMs - MAX_LOOKBACK_WINDOW,
            endTime: nowMs,
            // and limit the number of operations:
            limit: MAX_LOOKBACK_OPERATIONS,
        },
        skip: !qualityEntityUrn,
    });

    const operations = operationsData?.dataset?.operations;
    const mostRecentOperations = mostRecentOperationsTimeSinceInMillis(operations || []);
    const updateOperationsSummary = (
        <UpdateOperationsSummary>
            <Typography.Text>
                On average, this table updates every{' '}
                <b>{formatDuration(computeAverageUpdateFrequencyInMillis(operations || []))}</b>.
            </Typography.Text>
            <br />
            <Typography.Text>
                The latest {mostRecentOperations.length} updates were:{' '}
                {mostRecentOperations.map((operation, index) => (
                    <Tooltip title={`Updated ${formatTimestamp(operation.lastUpdatedTimestamp, DATE_COMMA_TIME_TZ)}`}>
                        <span key={operation.key}>
                            <b>{getFormattedTimeStringTimeSince(Date.now() - operation.delta, Date.now())}</b>
                            {index < mostRecentOperations.length - 1 ? ', ' : '.'}
                        </span>
                    </Tooltip>
                ))}
            </Typography.Text>
        </UpdateOperationsSummary>
    );

    const recentUpdatesSection = (
        <RecentUpdatesSection>
            <AssertionFormTitleAndTooltip
                formTitle="Recent table update history"
                tooltipTitle="Recent table update history"
                tooltipDescription={`This summary is computed using up to ${MAX_LOOKBACK_OPERATIONS} table update operations over the last ${MAX_LOOKBACK_WINDOW_DAYS} days.`}
                titleClassName="assertion-form-title"
            />
            {updateOperationsSummary}
        </RecentUpdatesSection>
    );

    return (
        <Form>
            {!canViewDatasetProfileFromQualityTabContext ||
            operationsError ||
            operationsLoading ||
            (operations && operations.length === 0)
                ? null
                : recentUpdatesSection}

            <Title level={5}>Pass if this table has updated...</Title>
            <RadioGroup
                value={scheduleType}
                onChange={(e: RadioChangeEvent) => handleScheduleTypeChange(e.target.value)}
                disabled={disabled}
            >
                <RadioContainer>
                    <StyledRadio value={FreshnessAssertionScheduleBuilderTypeOptions.SinceTheLastCheck}>
                        <TextContainer>
                            <Typography.Text strong>Since the previous check</Typography.Text>
                            <Typography.Text type="secondary">
                                We&apos;ll verify that this table has updated since the last scheduled check.
                            </Typography.Text>
                        </TextContainer>
                    </StyledRadio>
                </RadioContainer>
                <RadioContainer>
                    <StyledRadio value={FreshnessAssertionScheduleBuilderTypeOptions.FixedInterval}>
                        <TextContainer>
                            <FixedIntervalScheduleBuilder
                                value={fixedInterval}
                                onChange={handleFixedIntervalScheduleChange}
                                disabled={disabled}
                                // Prevents clicks inside Select dropdown from malfunctioning when nested inside Radio:
                                stopPropagation
                            />
                            {fixedInterval?.unit && fixedInterval?.multiple && (
                                <Typography.Text type="secondary">
                                    We&apos;ll verify that this table has updated within {fixedInterval?.multiple}{' '}
                                    {fixedInterval?.unit.toLocaleLowerCase()}s of the scheduled check.
                                </Typography.Text>
                            )}
                        </TextContainer>
                    </StyledRadio>
                </RadioContainer>
                {/* Cannot change an imperative freshness assertion to an inferred freshness assertion */}
                {isDetectWithAIEnabled && (
                    <RadioContainer>
                        <StyledRadio value={FreshnessAssertionScheduleBuilderTypeOptions.AiInferred}>
                            <TextContainer>
                                <Typography.Text strong>Detect with AI</Typography.Text>
                                <Typography.Text type="secondary">
                                    We&apos;ll verify that this table updates within the &apos;normal&apos; historical
                                    cadence. If there are any anomalies, the check will fail.
                                </Typography.Text>
                            </TextContainer>
                        </StyledRadio>
                    </RadioContainer>
                )}
            </RadioGroup>
        </Form>
    );
};
