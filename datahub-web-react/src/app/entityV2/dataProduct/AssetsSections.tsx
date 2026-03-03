import { AppstoreOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import ContentSectionLoading from '@app/entityV2/domain/summary/ContentSectionLoading';
import {
    getContentsSummary,
    getDomainEntitiesFilterUrl,
    navigateToDomainEntities,
} from '@app/entityV2/shared/containers/profile/sidebar/Domain/utils';
import { SummaryTabHeaderTitle, SummaryTabHeaderWrapper } from '@app/entityV2/shared/summary/HeaderComponents';
import { getContentTypeIcon } from '@app/entityV2/shared/summary/IconComponents';
import { HorizontalList } from '@app/entityV2/shared/summary/ListComponents';
import { pluralize } from '@app/shared/textUtil';
import { EntityCountCard } from '@app/sharedV2/cards/EntityCountCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListDataProductAssetsQuery } from '@graphql/search.generated';

const AssetsSectionWrapper = styled.div`
    flex: 1;
    min-width: 100px;
`;

export const StyledHeaderWrapper = styled(SummaryTabHeaderWrapper)`
    margin-bottom: 8px;
`;

export const AssetsSection = () => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { urn, entityType } = useEntityData();
    const { data, loading } = useListDataProductAssetsQuery({
        variables: {
            urn,
            input: {
                query: '*',
                start: 0,
                count: 0,
                filters: [],
            },
        },
    });

    const contentsSummary = data?.listDataProductAssets && getContentsSummary(data.listDataProductAssets);
    const contentsCount = contentsSummary?.total || 0;
    const hasContents = contentsCount > 0;

    if (!hasContents) {
        return null;
    }

    return (
        <AssetsSectionWrapper>
            <StyledHeaderWrapper>
                <SummaryTabHeaderTitle icon={<AppstoreOutlined />} title={`Assets (${contentsCount})`} />
                <Button type="link" onClick={() => navigateToDomainEntities(urn, entityType, history, entityRegistry)}>
                    View all
                </Button>
            </StyledHeaderWrapper>
            {loading && <ContentSectionLoading />}

            <HorizontalList>
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
            </HorizontalList>
        </AssetsSectionWrapper>
    );
};
