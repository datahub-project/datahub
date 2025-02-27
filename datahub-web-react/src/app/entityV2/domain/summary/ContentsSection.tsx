import React, { useEffect } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { AppstoreOutlined } from '@ant-design/icons';
import { useEntityContext, useEntityData } from '../../../entity/shared/EntityContext';
import { useGetDomainEntitySummaryQuery } from '../../../../graphql/domain.generated';
import {
    getContentsSummary,
    getDomainEntitiesFilterUrl,
    navigateToDomainEntities,
} from '../../shared/containers/profile/sidebar/Domain/utils';
import { useEntityRegistry } from '../../../useEntityRegistry';
import ContentSectionLoading from './ContentSectionLoading';
import { EntityCountCard } from '../../../sharedV2/cards/EntityCountCard';
import { pluralize } from '../../../shared/textUtil';
import {
    SectionContainer,
    SummaryTabHeaderTitle,
    SummaryTabHeaderWrapper,
} from '../../shared/summary/HeaderComponents';
import { getContentTypeIcon } from '../../shared/summary/IconComponents';
import { ANTD_GRAY } from '../../shared/constants';
import { Carousel } from '../../../sharedV2/carousel/Carousel';

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
