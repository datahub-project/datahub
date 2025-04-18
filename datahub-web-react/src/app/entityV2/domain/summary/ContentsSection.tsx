import { AppstoreOutlined } from '@ant-design/icons';
import React, { useEffect } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { useEntityContext, useEntityData } from '@app/entity/shared/EntityContext';
import ContentSectionLoading from '@app/entityV2/domain/summary/ContentSectionLoading';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import {
    getContentsSummary,
    getDomainEntitiesFilterUrl,
    navigateToDomainEntities,
} from '@app/entityV2/shared/containers/profile/sidebar/Domain/utils';
import {
    SectionContainer,
    SummaryTabHeaderTitle,
    SummaryTabHeaderWrapper,
} from '@app/entityV2/shared/summary/HeaderComponents';
import { getContentTypeIcon } from '@app/entityV2/shared/summary/IconComponents';
import { pluralize } from '@app/shared/textUtil';
import { EntityCountCard } from '@app/sharedV2/cards/EntityCountCard';
import { Carousel } from '@app/sharedV2/carousel/Carousel';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetDomainEntitySummaryQuery } from '@graphql/domain.generated';

const ViewAllButton = styled.div`
    color: ${ANTD_GRAY[7]};
    padding: 2px;
    :hover {
        cursor: pointer;
        color: ${ANTD_GRAY[8]};
        text-decoration: underline;
    }
`;

export const ContentsSection = () => {
    const { entityState } = useEntityContext();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { urn, entityType } = useEntityData();
    const { data, loading, refetch } = useGetDomainEntitySummaryQuery({
        variables: {
            urn,
        },
    });

    const contentsSummary = data?.aggregateAcrossEntities && getContentsSummary(data.aggregateAcrossEntities as any);
    const contentsCount = contentsSummary?.total || 0;
    const hasContents = contentsCount > 0;

    const shouldRefetch = entityState?.shouldRefetchContents;
    useEffect(() => {
        if (shouldRefetch) {
            refetch();
            entityState?.setShouldRefetchContents(false);
        }
    }, [shouldRefetch, entityState, refetch]);

    if (!hasContents) {
        return null;
    }

    return (
        <SectionContainer>
            <SummaryTabHeaderWrapper>
                <SummaryTabHeaderTitle icon={<AppstoreOutlined />} title={`Assets (${contentsCount})`} />
                <ViewAllButton onClick={() => navigateToDomainEntities(urn, entityType, history, entityRegistry)}>
                    View all
                </ViewAllButton>
            </SummaryTabHeaderWrapper>
            {loading && <ContentSectionLoading />}
            <Carousel>
                {!loading &&
                    contentsSummary?.types?.map((summary) => {
                        const { type, count, entityType: summaryEntityType } = summary;
                        const typeName = (
                            type ||
                            entityRegistry.getEntityName(summaryEntityType) ||
                            summaryEntityType
                        ).toLocaleLowerCase();
                        const link = getDomainEntitiesFilterUrl(
                            urn,
                            entityType,
                            entityRegistry,
                            [summary.entityType],
                            summary.type ? [summary.type] : undefined,
                        );
                        return (
                            <EntityCountCard
                                key={typeName}
                                type={typeName}
                                name={typeName}
                                count={summary.count}
                                icon={getContentTypeIcon(entityRegistry, summary.entityType, summary.type)}
                                tooltipDescriptor={pluralize(count, typeName)}
                                link={link}
                            />
                        );
                    })}
            </Carousel>
        </SectionContainer>
    );
};
