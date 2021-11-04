import { grey } from '@ant-design/colors';
import { Alert, Button, Card, Typography } from 'antd';
import React from 'react';
import { useHistory, useParams } from 'react-router';
import styled from 'styled-components';

import { useGetTagQuery } from '../../../graphql/tag.generated';
import { EntityType } from '../../../types.generated';
import { useGetAllEntitySearchResults } from '../../../utils/customGraphQL/useGetAllEntitySearchResults';
import { navigateToSearchUrl } from '../../search/utils/navigateToSearchUrl';
import { Message } from '../../shared/Message';
import { AvatarsGroup } from '../../shared/avatar';
import { useEntityRegistry } from '../../useEntityRegistry';

const PageContainer = styled.div`
    padding: 32px 100px;
`;

const LoadingMessage = styled(Message)`
    margin-top: 10%;
`;

const TitleLabel = styled(Typography.Text)`
    &&& {
        color: ${grey[2]};
        font-size: 13px;
    }
`;

const CreatedByLabel = styled(Typography.Text)`
    &&& {
        color: ${grey[2]};
        font-size: 13px;
    }
`;

const StatsLabel = styled(Typography.Text)`
    &&& {
        color: ${grey[2]};
        font-size: 13px;
    }
`;

const TitleText = styled(Typography.Title)`
    &&& {
        margin-top: 0px;
    }
`;

const HeaderLayout = styled.div`
    display: flex;
    justify-content: space-between;
`;

const StatsBox = styled.div`
    width: 180px;
    justify-content: left;
`;

const StatText = styled(Typography.Text)`
    font-size: 15px;
`;

const EmptyStatsText = styled(Typography.Text)`
    font-size: 15px;
    font-style: italic;
`;

const TagSearchButton = styled(Button)`
    margin-left: -16px;
`;

type TagPageParams = {
    urn: string;
};

/**
 * Responsible for displaying metadata about a tag
 */
export default function TagProfile() {
    const { urn: encodedUrn } = useParams<TagPageParams>();
    const urn = decodeURIComponent(encodedUrn);

    const { loading, error, data } = useGetTagQuery({ variables: { urn } });
    const entityRegistry = useEntityRegistry();
    const history = useHistory();

    const entityAndSchemaQuery = `tags:"${data?.tag?.name}" OR fieldTags:"${data?.tag?.name}" OR editedFieldTags:"${data?.tag?.name}"`;
    const entityQuery = `tags:"${data?.tag?.name}"`;

    const allSearchResultsByType = useGetAllEntitySearchResults({
        query: entityAndSchemaQuery,
        start: 0,
        count: 1,
        filters: [],
    });

    const statsLoading = Object.keys(allSearchResultsByType).some((type) => {
        return allSearchResultsByType[type].loading;
    });

    const someStats =
        !statsLoading &&
        Object.keys(allSearchResultsByType).some((type) => {
            return allSearchResultsByType[type]?.data?.search.total > 0;
        });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    return (
        <PageContainer>
            {loading && <LoadingMessage type="loading" content="Loading..." />}
            <Card
                title={
                    <HeaderLayout>
                        <div>
                            <div>
                                <TitleLabel>Tag</TitleLabel>
                                <TitleText>{data?.tag?.name}</TitleText>
                            </div>
                            <div>
                                <div>
                                    <CreatedByLabel>Created by</CreatedByLabel>
                                </div>
                                <AvatarsGroup owners={data?.tag?.ownership?.owners} entityRegistry={entityRegistry} />
                            </div>
                        </div>
                        <StatsBox>
                            <StatsLabel>Applied to</StatsLabel>
                            {statsLoading && (
                                <div>
                                    <EmptyStatsText>Loading...</EmptyStatsText>
                                </div>
                            )}
                            {!statsLoading && !someStats && (
                                <div>
                                    <EmptyStatsText>No entities</EmptyStatsText>
                                </div>
                            )}
                            {!statsLoading &&
                                someStats &&
                                Object.keys(allSearchResultsByType).map((type) => {
                                    if (allSearchResultsByType[type]?.data?.search.total === 0) {
                                        return null;
                                    }
                                    return (
                                        <div key={type}>
                                            <TagSearchButton
                                                type="text"
                                                key={type}
                                                onClick={() =>
                                                    navigateToSearchUrl({
                                                        type: type as EntityType,
                                                        query:
                                                            type === EntityType.Dataset
                                                                ? entityAndSchemaQuery
                                                                : entityQuery,
                                                        history,
                                                    })
                                                }
                                            >
                                                <StatText data-testid={`stats-${type}`}>
                                                    {allSearchResultsByType[type]?.data?.search.total}{' '}
                                                    {entityRegistry.getCollectionName(type as EntityType)}
                                                </StatText>
                                            </TagSearchButton>
                                        </div>
                                    );
                                })}
                        </StatsBox>
                    </HeaderLayout>
                }
            >
                <Typography.Paragraph strong style={{ color: grey[2], fontSize: 13 }}>
                    Description
                </Typography.Paragraph>
                <Typography.Text>{data?.tag?.description}</Typography.Text>
            </Card>
        </PageContainer>
    );
}
