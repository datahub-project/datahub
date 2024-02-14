import React from 'react';
import { ToolTwoTone } from '@ant-design/icons';
import { buildMostUpdatedFilters, buildMostUpdatedSort } from './useGetMostUpdated';
import { SearchListInsightCard } from './SearchListInsightCard';
import { EntityType } from '../../../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../../../../entityV2/shared/constants';

export const MostFrequentlyUpdated = () => {
    return (
        <SearchListInsightCard
            icon={<ToolTwoTone style={{ color: REDESIGN_COLORS.GREY }} />}
            tip="Tables with the most changes in the past month"
            title="Most Updated"
            types={[EntityType.Dataset]}
            filters={buildMostUpdatedFilters()}
            sort={buildMostUpdatedSort()}
        />
    );
};
