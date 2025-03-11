import React from 'react';
import { HealthSummaryCard } from '../HealthSummaryCard';

// In the future we'll want incidents assigned to you specifically. So we almost need sections / headers inside the cards.
// Or assertions you've created or subscribed to.

type Props = {
    summary: number;
    loading: boolean;
};

export const IncidentSummary = ({ loading, summary }: Props) => {
    return <HealthSummaryCard loading={loading}>{summary}</HealthSummaryCard>;
};
