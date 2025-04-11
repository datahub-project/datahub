import { useIsSeparateSiblingsMode } from '@src/app/entity/shared/siblingUtils';
import Icon from '@ant-design/icons';
import { Button, Typography } from 'antd';
import moment from 'moment';
import React, { useEffect } from 'react';
import styled from 'styled-components';
import NoStatsAvailble from '../../../../../../../../images/no-stats-available.svg?react';
import { useBaseEntity, useRouteToTab } from '../../../../../../../entity/shared/EntityContext';
import { ANTD_GRAY } from '../../../../../constants';
import Query from '../../../Queries/Query';
import { PopularityColumn, QueryCreatedBy } from '../../../Queries/queryColumns';
import { usePopularQueries } from '../../../Queries/usePopularQueries';
import { GetDatasetQuery } from '../../../../../../../../graphql/dataset.generated';
import Loading from '../../../../../../../shared/Loading';
import { generateSchemaFieldUrn } from '../../../../Lineage/utils';

interface Props {
    properties: {
        fieldPath: string;
    };
}

const StyledCreatedBy = styled.div`
    margin-top: -2.5px;
    margin-left: 4px;
    margin-right: 4px;
`;

const StyledQueryContainer = styled.div`
    margin-top: 10px;
    display: flex;
    flex-direction: column;
    max-width: 100%;
`;

const StyledQueryCard = styled.div`
    background: #f5f5f5;
    border-radius: 4px;
    margin-bottom: 10px;
    margin-left: 10px;
    margin-right: 10px;
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 4px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
`;

const QuerySubtitleContainer = styled.div`
    align-items: center;
    padding: 10px;
    display: flex;
    flex-direction: row;
    justify-content: space-between;
`;

const SubtitleSection = styled.div`
    display: flex;
    flex-direction: row;
`;

const PopularityLabel = styled.span`
    line-height: 26px;
    margin-right: 8px;
`;

const PopularityColumnContainer = styled.div`
    margin-bottom: 5px;
`;

const QUERIES_TO_SHOW = 6;

const SeeAllButton = styled(Button)`
    margin-top: 10px;
    margin-left: 10px;
    margin-right: 10px;
    margin-bottom: 50px;
    width: 170px;
`;

const QueriesTabContainer = styled.div`
    display: flex;
    flex-direction: column;
    max-width: 485px;
    text-align: center;
    align-items: center;
    margin: auto;
`;

const NoDataContainer = styled.div`
    margin: 40px auto;
    display: flex;
    flex-direction: column;
    align-items: center;
`;

const StyledIcon = styled(Icon)`
    font-size: 80px;
    margin-bottom: 6px;
    color: #fff;
`;

const Section = styled.div`
    color: #56668e;
    font-weight: 700;
    font-size: 12px;
    line-height: 24px;
`;

const QueriesTitle = styled(Typography.Text)`
    && {
        margin: 0px;
        font-size: 16px;
        font-weight: 700;
        align-self: flex-start;
        margin: 10px 0 0 10px;
    }
`;

export default function SchemaFieldQueriesSidebarTab({ properties: { fieldPath } }: Props) {
    const isSeparateSiblings = useIsSeparateSiblingsMode();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const urn = (baseEntity && baseEntity.dataset && baseEntity.dataset?.urn) || '';
    const siblingUrn = isSeparateSiblings
        ? undefined
        : baseEntity?.dataset?.siblingsSearch?.searchResults?.[0]?.entity?.urn;
    const schemaFieldUrn = generateSchemaFieldUrn(fieldPath, urn) || '';
    const siblingSchemaFieldUrn =
        !isSeparateSiblings && siblingUrn ? generateSchemaFieldUrn(fieldPath, siblingUrn) || '' : '';

    const { popularQueries, loading, total, selectedColumnsFilter, setSelectedColumnsFilter } = usePopularQueries({
        entityUrn: urn,
        siblingUrn,
        filterText: '',
        defaultSelectedColumns: [schemaFieldUrn || '', siblingSchemaFieldUrn],
    });

    useEffect(() => {
        setSelectedColumnsFilter({ ...selectedColumnsFilter, values: [schemaFieldUrn, siblingSchemaFieldUrn] });
        // disable next line because we ONLY want this to run when schemaFieldUrn or siblingSchemaFieldUrn changes
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [schemaFieldUrn, siblingSchemaFieldUrn]);

    const routeToTab = useRouteToTab();

    const firstQueries = popularQueries.slice(0, QUERIES_TO_SHOW);

    const hasMore = total > firstQueries.length;

    const hasNoQueries = popularQueries.length === 0;

    return (
        <QueriesTabContainer>
            {!loading && hasNoQueries && (
                <NoDataContainer>
                    <StyledIcon component={NoStatsAvailble} />
                    <Section>No queries for this column found</Section>
                </NoDataContainer>
            )}
            {loading && <Loading />}
            {!loading && firstQueries.length > 0 && <QueriesTitle>Queries</QueriesTitle>}

            {firstQueries.length > 0 &&
                firstQueries.map((query, idx) => (
                    <StyledQueryContainer>
                        <StyledQueryCard>
                            <Query query={query.query} index={idx} isCompact showDetails={false} showHeader={false} />
                            <QuerySubtitleContainer>
                                <SubtitleSection>
                                    Last run by{' '}
                                    {query.createdBy && (
                                        <StyledCreatedBy>
                                            <QueryCreatedBy createdBy={query.createdBy} />
                                        </StyledCreatedBy>
                                    )}
                                    on {moment(query.lastRun).format('MM/DD/YYYY')}
                                </SubtitleSection>
                                <SubtitleSection>
                                    <PopularityLabel>Popularity</PopularityLabel>
                                    <PopularityColumnContainer>
                                        <PopularityColumn query={query} />
                                    </PopularityColumnContainer>
                                </SubtitleSection>
                            </QuerySubtitleContainer>
                        </StyledQueryCard>
                    </StyledQueryContainer>
                ))}
            {hasMore && !loading && (
                <SeeAllButton
                    onClick={() => {
                        routeToTab({
                            tabName: 'Queries',
                            tabParams: { column: schemaFieldUrn, siblingColumn: siblingSchemaFieldUrn },
                        });
                    }}
                >
                    See All Queries
                </SeeAllButton>
            )}
        </QueriesTabContainer>
    );
}
