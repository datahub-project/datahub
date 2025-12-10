import { Modal, Text } from '@components';
import moment from 'moment';
import React, { useRef, useState } from 'react';

import { DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/LookBackWindowAdjuster';
import { TuningHelpBanner } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/TuningHelpBanner';
import { DataFreshnessChart } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/freshness/DataFreshnessChart';
import { LAST_UPDATED_TIMESTAMP_FILTER_NAME, OPERATION_TYPE_FILTER_NAME } from '@app/searchV2/utils/constants';

import { useGetOperationsQuery } from '@graphql/dataset.generated';
import { Assertion, FilterOperator, Monitor, OperationType } from '@types';

type Props = {
    onClose: () => void;
    assertion: Assertion;
    monitor: Monitor;
};

export const TuneFreshnessAssertionModal = ({ onClose, assertion, monitor: _monitor }: Props) => {
    const nowRef = useRef(Date.now());
    const originalRangeRef = useRef({
        start: nowRef.current - DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS * 24 * 60 * 60 * 1000,
        end: nowRef.current,
    });
    const [range, setRange] = useState<{ start: number; end: number }>(originalRangeRef.current);

    const entityUrn = assertion.info?.entityUrn;

    if (!entityUrn) {
        throw new Error('Entity URN is required to tune freshness assertion.');
    }

    const { data, loading, error } = useGetOperationsQuery({
        variables: {
            urn: entityUrn,
            filters: {
                and: [
                    {
                        field: LAST_UPDATED_TIMESTAMP_FILTER_NAME,
                        condition: FilterOperator.GreaterThanOrEqualTo,
                        // We use the original range to ensure we get all
                        // operations within the lookback window. Filtering is
                        // done in the chart component.
                        values: [originalRangeRef.current.start.toString()],
                    },
                    {
                        field: LAST_UPDATED_TIMESTAMP_FILTER_NAME,
                        condition: FilterOperator.LessThanOrEqualTo,
                        values: [originalRangeRef.current.end.toString()],
                    },
                    {
                        field: OPERATION_TYPE_FILTER_NAME,
                        condition: FilterOperator.In,
                        // We only want to track operations that can be counted
                        // as "FRESHNESS" events, as in there is new data in the
                        // table
                        values: [OperationType.Insert, OperationType.Create, OperationType.Update],
                    },
                ],
            },
        },
    });

    const operations = data?.dataset?.operations || [];

    // Convert current range to moment objects for the date picker
    const currentDateRange = [range.start ? moment(range.start) : null, range.end ? moment(range.end) : null] as [
        moment.Moment | null,
        moment.Moment | null,
    ];

    // Handle date range changes from the picker
    const handleDateRangeChange = (startDate: moment.Moment | null, endDate: moment.Moment | null) => {
        if (startDate && endDate) {
            setRange({
                start: startDate.clone().startOf('day').valueOf(),
                end: endDate.clone().endOf('day').valueOf(),
            });
        }
    };

    // Check if current range differs from original range
    const hasRangeChanged =
        range.start !== originalRangeRef.current.start || range.end !== originalRangeRef.current.end;

    return (
        <Modal
            title="Tune Predictions"
            onCancel={onClose}
            subtitle="View historical dataset update events to understand update patterns."
            width={800}
            open
            style={{ margin: `16px 0` }}
            bodyStyle={{ maxHeight: '80vh', overflowY: 'auto' }}
            buttons={[
                {
                    text: 'Done',
                    onClick: onClose,
                },
            ]}
        >
            <TuningHelpBanner />
            {error && (
                <div style={{ marginBottom: 16, padding: 12, background: '#fff2f0', borderRadius: 4 }}>
                    <Text color="red" size="sm">
                        {error.message || 'Failed to load dataset update events'}
                    </Text>
                </div>
            )}
            <DataFreshnessChart
                operations={operations}
                loading={loading}
                width={750}
                height={400}
                onRangeChange={setRange}
                resetRange={hasRangeChanged ? () => setRange(originalRangeRef.current) : undefined}
                onDateRangeChange={handleDateRangeChange}
                dateRange={currentDateRange}
                currentTime={nowRef.current}
            />
        </Modal>
    );
};
