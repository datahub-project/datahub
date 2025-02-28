import React from 'react';
import styled from 'styled-components';
import { GetChartQuery } from '../../../../graphql/chart.generated';
import { Entity, EntityType } from '../../../../types.generated';
import { useBaseEntity, useEntityData } from '../../../entity/shared/EntityContext';
import Loading from '../../../shared/Loading';
import SummaryEntityCard from '../../../sharedV2/cards/SummaryEntityCard';
import { useEntityRegistryV2 } from '../../../useEntityRegistry';
import { SubType } from '../../shared/components/subtypes';
import { HorizontalList, SummaryColumns } from '../../shared/summary/ListComponents';
import SummaryCreatedBySection from '../../shared/summary/SummaryCreatedBySection';
import SummaryQuerySection from './SummaryQuerySection';
import { MainSection, StyledTitle, SummaryHeader, VerticalDivider } from './styledComponents';

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

const FirstRow = styled.div`
    display: flex;
`;

const SectionContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

export default function ChartSummaryOverview() {
    const { loading } = useEntityData();
    const chart = useBaseEntity<GetChartQuery>()?.chart;
    const entityRegistry = useEntityRegistryV2();

    // TODO: Fix casting
    // TODO: Check workbook + data source platform actually matches this entity's platform
    const workbook = chart?.parentContainers?.containers?.find((c) =>
        c.subTypes?.typeNames?.includes(SubType.TableauWorkbook),
    ) as Entity;

    // TODO: Calculate this better?
    const dataSources = (chart?.inputs?.relationships
        ?.map((r) => r.entity)
        ?.filter((e) => e?.__typename === 'Dataset') || []) as Entity[];

    const dashboards = (chart?.dashboards?.relationships?.map((r) => r.entity) || []) as Entity[];

    const owner = chart?.ownership?.owners && chart?.ownership?.owners[0]?.owner;

    const query = chart?.query?.rawQuery || '';

    if (loading) {
        return <Loading />;
    }

    return (
        <SummaryColumns>
            <MainSection>
                <SummaryHeader>General Info</SummaryHeader>
                <FirstRow>
                    {!!owner && <SummaryCreatedBySection owner={owner} />}

                    {!!dataSources?.length && (
                        <>
                            <VerticalDivider />

                            <MainSection>
                                <StyledTitle>
                                    Data Sources
                                    <Count>{dataSources.length} </Count>
                                </StyledTitle>
                                <HorizontalList>
                                    {dataSources.map((dataSource) => (
                                        <SummaryEntityCard key={dataSource.urn} entity={dataSource} />
                                    ))}
                                </HorizontalList>
                            </MainSection>
                        </>
                    )}
                    {!!query && (
                        <>
                            <VerticalDivider />

                            <SectionContainer>
                                <StyledTitle>Query</StyledTitle>
                                <SummaryQuerySection query={query} />
                            </SectionContainer>
                        </>
                    )}
                </FirstRow>
                {workbook && (
                    <>
                        <StyledTitle>{SubType.TableauWorkbook}</StyledTitle>
                        <SummaryEntityCard entity={workbook} />
                    </>
                )}
            </MainSection>
            {!!dashboards?.length && (
                <>
                    <StyledTitle>
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
