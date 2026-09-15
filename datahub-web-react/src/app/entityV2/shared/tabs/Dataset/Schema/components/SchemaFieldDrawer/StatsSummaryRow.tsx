import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import TrendDetail from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/TrendDetail';

import { DatasetFieldProfile } from '@types';

const StatsSummaryRowContent = styled.div`
    display: flex;
    flex-direction: row;
    align-items: flex-start;
`;

interface Props {
    fieldProfile: DatasetFieldProfile | undefined;
}

export default function StatsSummaryRow({ fieldProfile }: Props) {
    const { t } = useTranslation('entity.profile.schema');
    const nullProportion = fieldProfile?.nullProportion;
    const nullCount = fieldProfile?.nullCount;
    const uniqueProportion = fieldProfile?.uniqueProportion;
    const uniqueCount = fieldProfile?.uniqueCount;

    const numericalStatsCount =
        (fieldProfile?.min !== null && fieldProfile?.min !== undefined ? 1 : 0) +
        (fieldProfile?.max !== null && fieldProfile?.max !== undefined ? 1 : 0) +
        (fieldProfile?.mean !== null && fieldProfile?.mean !== undefined ? 1 : 0) +
        (fieldProfile?.median !== null && fieldProfile?.median !== undefined ? 1 : 0) +
        (fieldProfile?.stdev !== null && fieldProfile?.stdev !== undefined ? 1 : 0);

    return (
        <StatsSummaryRowContent>
            <TrendDetail
                tooltipTitle={t('statsSummary.nullValuesTooltip', {
                    count: nullCount ?? 0,
                })}
                headline={t('statsSummary.nullValues')}
                proportion={nullProportion}
                showCount
                count={nullCount || 0}
            />
            <TrendDetail
                tooltipTitle={t('statsSummary.uniqueValuesTooltip', {
                    count: uniqueCount ?? 0,
                })}
                headline={t('statsSummary.distinctValues')}
                proportion={uniqueProportion}
                showCount
                count={uniqueCount || 0}
            />
            <TrendDetail
                tooltipTitle=""
                headline={t('statsSummary.numericalStats')}
                proportion={undefined}
                showCount
                count={numericalStatsCount}
            />
        </StatsSummaryRowContent>
    );
}
