import React from 'react';
import styled from 'styled-components';
import { HeaderTitle } from '../../shared/summary/HeaderComponents';
import { GetChartQuery } from '../../../../graphql/chart.generated';
import { Entity, EntityType } from '../../../../types.generated';
import { useEntityRegistryV2 } from '../../../useEntityRegistry';
import { useBaseEntity } from '../../../entity/shared/EntityContext';
import { getSubTypeIcon, SubType } from '../../shared/components/subtypes';
import { HorizontalList, SummaryColumns } from '../../shared/summary/ListComponents';
import SummaryEntityCard from '../../../sharedV2/cards/SummaryEntityCard';
import { REDESIGN_COLORS } from '../../shared/constants';

const StyledTitle = styled(HeaderTitle)`
    margin-bottom: 12px;
    color: ${REDESIGN_COLORS.SUBTITLE};
    font-weight: 700;
`;

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

export default function ChartSummaryOverview() {
    const chart = useBaseEntity<GetChartQuery>()?.chart;
    const entityRegistry = useEntityRegistryV2();

    // TODO: Fix casting
    // TODO: Check workbook + data source platform actually matches this entity's platform
    const workbook = chart?.parentContainers?.containers?.find((c) =>
        c.subTypes?.typeNames?.includes(SubType.TableauWorkbook),
    ) as Entity;

    // TODO: Calculate this better?
    const dataSources = chart?.inputs?.relationships
        ?.map((r) => r.entity)
        .filter((e) => e?.__typename === 'Dataset') as Entity[];
    const dashboards = chart?.dashboards?.relationships?.map((r) => r.entity) as Entity[];

    return (
        <SummaryColumns>
            {workbook && (
                <>
                    <StyledTitle>
                        {getSubTypeIcon(SubType.TableauWorkbook)}
                        {SubType.TableauWorkbook}
                    </StyledTitle>
                    <SummaryEntityCard entity={workbook} />
                </>
            )}
            {!!dataSources?.length && (
                <>
                    <StyledTitle>
                        {getSubTypeIcon(SubType.TableauPublishedDataSource)}
                        Data Sources
                        <Count>{dataSources.length} </Count>
                    </StyledTitle>
                    <HorizontalList>
                        {dataSources.map((dataSource) => (
                            <SummaryEntityCard key={dataSource.urn} entity={dataSource} />
                        ))}
                    </HorizontalList>
                </>
            )}
            {!!dashboards?.length && (
                <>
                    <StyledTitle>
                        {entityRegistry.getIcon(EntityType.Dashboard)}
                        {entityRegistry.getEntityName(EntityType.Dashboard)}
                        <Count>{dashboards?.length}</Count>
                    </StyledTitle>
                    <HorizontalList>
                        {dashboards.map((dashboard) => (
                            <SummaryEntityCard key={dashboard.urn} entity={dashboard} />
                        ))}
                    </HorizontalList>
                </>
            )}
        </SummaryColumns>
    );
}
