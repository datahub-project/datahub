import { Button } from 'antd';
import React from 'react';
import { useHistory } from 'react-router';
import { AppstoreOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { useListDataProductAssetsQuery } from '../../../graphql/search.generated';
import { pluralize } from '../../shared/textUtil';
import { EntityCountCard } from '../../sharedV2/cards/EntityCountCard';
import { useEntityRegistry } from '../../useEntityRegistry';
import ContentSectionLoading from '../domain/summary/ContentSectionLoading';
import { useEntityData } from '../../entity/shared/EntityContext';
import {
    getContentsSummary,
    getDomainEntitiesFilterUrl,
    navigateToDomainEntities,
} from '../shared/containers/profile/sidebar/Domain/utils';
import { SummaryTabHeaderTitle, SummaryTabHeaderWrapper } from '../shared/summary/HeaderComponents';
import { HorizontalList } from '../shared/summary/ListComponents';
import { getContentTypeIcon } from '../shared/summary/IconComponents';

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
