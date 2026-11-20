import { EmptyState, Heading, Pagination } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import BrowseResultCard from '@app/browse/BrowseResultCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { BrowseResultGroup, Entity, EntityType } from '@types';

const Content = styled.main`
    padding: 25px 100px;
`;

const Results = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const EntityList = styled.div`
    width: 100%;
    margin-top: 12px;
    padding: 16px 32px;
    border: 1px solid ${(props) => props.theme.colors.border};
    box-shadow: ${(props) => props.theme.colors.shadowSm};
`;

const EntityListItem = styled.div`
    cursor: pointer;
`;

const Divider = styled.hr`
    margin: 8px 0;
    border: 0;
    border-top: 1px solid ${(props) => props.theme.colors.border};
`;

interface Props {
    type: EntityType;
    title: string;
    rootPath: string;
    page: number;
    pageSize: number;
    totalResults: number;
    groups: Array<BrowseResultGroup>;
    entities: Array<Entity>;
    onChangePage: (page: number) => void;
}

/**
 * Display browse groups + entities.
 */
export const BrowseResults = ({
    type,
    title,
    rootPath,
    page,
    pageSize,
    totalResults,
    entities,
    groups,
    onChangePage,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const { t } = useTranslation('misc');

    const onGroupClick = (group: BrowseResultGroup) => {
        analytics.event({
            type: EventType.BrowseResultClickEvent,
            browsePath: rootPath,
            entityType: type,
            resultType: 'Group',
            groupName: group.name,
        });
    };

    const onEntityClick = (entity: Entity) => {
        analytics.event({
            type: EventType.BrowseResultClickEvent,
            browsePath: rootPath,
            entityType: type,
            resultType: 'Entity',
            entityUrn: entity.urn,
        });
    };

    return (
        <div>
            <Content>
                <Heading type="h1">{title}</Heading>
                <Results>
                    {groups.map((group) => (
                        <div key={`${group.name}_key`}>
                            <BrowseResultCard
                                onClick={() => onGroupClick(group)}
                                name={group.name}
                                count={group.count}
                                url={`${rootPath}/${group.name}`}
                                type={entityRegistry.getCollectionName(type)}
                            />
                        </div>
                    ))}
                    {(!(groups && groups.length > 0) || (entities && entities.length > 0)) && (
                        <EntityList>
                            {entities.length ? (
                                entities.map((item, index) => (
                                    <React.Fragment key={item.urn}>
                                        <EntityListItem onClick={() => onEntityClick(item)}>
                                            {entityRegistry.renderBrowse(type, item)}
                                        </EntityListItem>
                                        {index < entities.length - 1 && <Divider />}
                                    </React.Fragment>
                                ))
                            ) : (
                                <EmptyState title={t('browse.noEntitiesEmpty')} size="sm" />
                            )}
                        </EntityList>
                    )}
                    <div>
                        <Pagination
                            currentPage={page}
                            itemsPerPage={pageSize}
                            total={totalResults}
                            showTitle
                            showLessItems
                            onPageChange={onChangePage}
                            showSizeChanger={false}
                        />
                    </div>
                </Results>
            </Content>
        </div>
    );
};
