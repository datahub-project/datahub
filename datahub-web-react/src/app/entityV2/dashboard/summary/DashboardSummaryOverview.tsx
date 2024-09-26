import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { GetDashboardQuery } from '../../../../graphql/dashboard.generated';
import { Entity, EntityType } from '../../../../types.generated';
import { useBaseEntity, useEntityData } from '../../../entity/shared/EntityContext';
import { GenericEntityProperties } from '../../../entity/shared/types';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import PlatformIcon from '../../../sharedV2/icons/PlatformIcon';
import { useEntityRegistryV2 } from '../../../useEntityRegistry';
import { MainSection, StyledTitle, SummaryHeader, VerticalDivider } from '../../chart/summary/styledComponents';
import { REDESIGN_COLORS } from '../../shared/constants';
import { SummaryColumns } from '../../shared/summary/ListComponents';
import SummaryCreatedBySection from '../../shared/summary/SummaryCreatedBySection';

import { useGetSearchResultsQuery } from '../../../../graphql/search.generated';
import Loading from '../../../shared/Loading';

const Count = styled.div`
    padding: 1px 8px;
    display: flex;
    justify-content: center;
    border-radius: 10px;
    background-color: #e5ece9;
    font-size: 10px;
    font-weight: 400;
    margin-left: 8px;
`;

const EntityItem = styled.div`
    display: flex;
    align-items: center;
    padding: 6px 40px 6px 0;
    gap: 8px;
    font-size: 14px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.SUBTITLE};
`;

const AssetSections = styled.div`
    display: flex;
`;

const EntitiesList = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
    max-height: 220px;
`;

export default function DashboardSummaryOverview() {
    const { loading } = useEntityData();
    const dashboard = useBaseEntity<GetDashboardQuery>()?.dashboard;
    const entityRegistry = useEntityRegistryV2();

    const charts = (dashboard?.charts?.relationships?.map((r) => r.entity) || []) as Entity[];

    const sources = charts
        .flatMap((chart: any) => chart?.upstream?.relationships?.map((r) => r.entity))
        .filter((e) => e.type === EntityType.Dataset)
        .map((dataSource) => dataSource.urn);

    const { data: dataSourcesData } = useGetSearchResultsQuery({
        variables: {
            input: {
                type: EntityType.Dataset,
                query: '',
                filters: [
                    {
                        field: 'urn',
                        values: sources,
                    },
                ],
            },
        },
    });

    if (loading) {
        return <Loading />;
    }

    const dataSources = (dataSourcesData?.search?.searchResults?.map((result) => result.entity) || []) as Entity[];

    const owner = dashboard?.ownership?.owners && dashboard?.ownership?.owners[0]?.owner;
    const displayName = entityRegistry.getDisplayName(EntityType.Dashboard, dashboard);

    return (
        <SummaryColumns>
            <MainSection>
                <SummaryHeader>General Info</SummaryHeader>

                {!!owner && <SummaryCreatedBySection owner={owner} />}
            </MainSection>

            <MainSection>
                <SummaryHeader>Related Assets</SummaryHeader>
                <AssetSections>
                    {!!dataSources?.length && (
                        <MainSection>
                            <StyledTitle>
                                Data Sources
                                <Count>{dataSources.length} </Count>
                            </StyledTitle>
                            <EntitiesList>
                                {dataSources.map((dataSource) => (
                                    <Link to={entityRegistry.getEntityUrl(dataSource.type, dataSource.urn)}>
                                        <HoverEntityTooltip placement="bottom" entity={dataSource} showArrow={false}>
                                            <EntityItem>
                                                <PlatformIcon
                                                    platform={(dataSource as GenericEntityProperties)?.platform}
                                                    size={18}
                                                    alt={displayName}
                                                    entityType={dataSource.type as EntityType}
                                                />
                                                {entityRegistry.getDisplayName(
                                                    dataSource?.type as EntityType,
                                                    dataSource,
                                                )}
                                            </EntityItem>
                                        </HoverEntityTooltip>
                                    </Link>
                                ))}
                            </EntitiesList>
                        </MainSection>
                    )}

                    <VerticalDivider />
                    {!!charts?.length && (
                        <MainSection>
                            <StyledTitle>
                                Contents
                                <Count>{charts.length} </Count>
                            </StyledTitle>
                            <EntitiesList>
                                {charts.map((chart) => (
                                    <Link to={entityRegistry.getEntityUrl(chart.type, chart.urn)}>
                                        <HoverEntityTooltip placement="bottom" entity={chart} showArrow={false}>
                                            <EntityItem>
                                                <PlatformIcon
                                                    platform={(chart as GenericEntityProperties).platform}
                                                    size={18}
                                                    alt={displayName}
                                                    entityType={chart.type as EntityType}
                                                />
                                                {entityRegistry.getDisplayName(chart?.type as EntityType, chart)}
                                            </EntityItem>
                                        </HoverEntityTooltip>
                                    </Link>
                                ))}
                            </EntitiesList>
                        </MainSection>
                    )}
                </AssetSections>
            </MainSection>
        </SummaryColumns>
    );
}
