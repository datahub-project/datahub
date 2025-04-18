import React from 'react';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import LastMonthStats from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/LastMonthStats';
import LatestStats from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/LatestStats';
import SelectSiblingDropdown from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/SelectSiblingDropdown';
import {
    Header,
    StatsContainer,
    VerticalDivider,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/styledComponents';
import { useGetStatsData } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/useGetStatsData';
import { PageTitle } from '@src/alchemy-components';
import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { GetDatasetQuery } from '@src/graphql/dataset.generated';
import { Dataset } from '@src/types.generated';

const StatsHighlights = () => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const baseEntityData = baseEntity.dataset as Dataset;

    const { statsEntityUrn, setStatsEntityUrn } = useStatsSectionsContext();

    const { isSiblingsMode } = useGetStatsData();

    return (
        <>
            <Header>
                <PageTitle
                    title="Highlights"
                    subTitle="View the latest statistics for this table"
                    variant="sectionHeader"
                />
                {isSiblingsMode && baseEntityData && (
                    <SelectSiblingDropdown
                        baseEntity={baseEntityData}
                        selectedSiblingUrn={statsEntityUrn}
                        setSelectedSiblingUrn={setStatsEntityUrn}
                    />
                )}
            </Header>
            <StatsContainer>
                <LatestStats />
                <VerticalDivider type="vertical" />
                <LastMonthStats />
            </StatsContainer>
        </>
    );
};

export default StatsHighlights;
