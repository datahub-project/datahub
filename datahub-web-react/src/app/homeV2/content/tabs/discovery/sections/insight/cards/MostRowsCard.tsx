import React from 'react';
import { RiseOutlined } from '@ant-design/icons';
import { SearchListInsightCard } from './SearchListInsightCard';
import { buildMostRowsFilters, buildMostRowsSort } from './useGetMostRows';
import { EntityType } from '../../../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../../../../entityV2/shared/constants';

export const MostRowsCard = () => {
    return (
        <SearchListInsightCard
            icon={<RiseOutlined style={{ color: REDESIGN_COLORS.BLUE }} />}
            title="Largest Tables by Rows"
            types={[EntityType.Dataset]}
            filters={buildMostRowsFilters()}
            sort={buildMostRowsSort()}
        />
    );
};
