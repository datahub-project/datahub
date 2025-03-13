import React from 'react';
import { HealthSummaryCard } from '../HealthSummaryCard';

type Props = {
    summary: number;
    loading: boolean;
};

export const AssertionSummary = ({ loading, summary }: Props) => {
    return <HealthSummaryCard loading={loading}>{summary}</HealthSummaryCard>;
};
