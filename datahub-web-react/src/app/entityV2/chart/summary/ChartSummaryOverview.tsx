import React from 'react';
import styled from 'styled-components';
import { HeaderTitle } from '../../shared/summary/HeaderComponents';
import { GetChartQuery } from '../../../../graphql/chart.generated';
import { Entity, EntityType } from '../../../../types.generated';
import { useEntityRegistryV2 } from '../../../useEntityRegistry';
import { useBaseEntity } from '../../shared/EntityContext';
import { getSubTypeIcon, SubType } from '../../shared/components/subtypes';
import { HorizontalList, SummaryColumns } from '../../shared/summary/ListComponents';
import SummaryEntityCard from '../../../sharedV2/cards/SummaryEntityCard';

const StyledTitle = styled(HeaderTitle)`
    margin-bottom: 12px;
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
                        Data Sources ({dataSources.length})
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
                        {`${entityRegistry.getEntityName(EntityType.Dashboard)} (${dashboards?.length})`}
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
