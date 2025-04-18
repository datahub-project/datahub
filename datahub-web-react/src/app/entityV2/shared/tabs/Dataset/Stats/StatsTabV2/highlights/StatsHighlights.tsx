import { PageTitle } from '@src/alchemy-components';
import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { GetDatasetQuery } from '@src/graphql/dataset.generated';
import { Dataset } from '@src/types.generated';
import React from 'react';
import { useStatsSectionsContext } from '../StatsSectionsContext';
import { useGetStatsData } from '../useGetStatsData';
import LastMonthStats from './LastMonthStats';
import LatestStats from './LatestStats';
import SelectSiblingDropdown from './SelectSiblingDropdown';
import { Header, StatsContainer, VerticalDivider } from './styledComponents';

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
