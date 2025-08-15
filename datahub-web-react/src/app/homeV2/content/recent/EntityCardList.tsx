import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import analytics, { EventType, HomePageModule } from '@app/analytics';
import { HorizontalList } from '@app/entityV2/shared/summary/ListComponents';
import { EntityCard } from '@app/homeV2/content/recent/EntityCard';

import { Entity } from '@types';

const MAX_ASSETS_TO_SHOW = 5;

const Container = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
    overflow: hidden;
`;

const Title = styled.div`
    color: #403d5c;
    margin: 0px;
    font-size: 18px;
    font-weight: 600;
`;

const Logo = styled.div`
    margin-right: 8px;
    display: flex;
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    margin-bottom: 16px;
`;

type Props = {
    title: string;
    logo?: React.ReactNode;
    tip?: React.ReactNode;
    entities: Entity[];
    max?: number;
    isHomePage?: boolean;
};

export const EntityCardList = ({ title, logo, tip, entities, max, isHomePage }: Props) => {
    if (entities.length === 0) {
        return null;
    }

    function handleCardClick(entity: Entity) {
        if (isHomePage) {
            analytics.event({
                type: EventType.HomePageClick,
                module: HomePageModule.YouRecentlyViewed,
                value: entity.urn,
            });
        }
    }

    const visibleEntities = max ? entities.slice(0, MAX_ASSETS_TO_SHOW) : entities;

    return (
        <Container>
            <Header>
                {logo && <Logo>{logo}</Logo>}
                <Title>
                    <Tooltip title={tip} showArrow={false} placement="right">
                        {title}
                    </Tooltip>
                </Title>
            </Header>
            <HorizontalList>
                {visibleEntities.map((entity) => (
                    // eslint-disable-next-line
                    <span onClick={() => handleCardClick(entity)} key={`${title}-${entity.urn}`}>
                        <EntityCard entity={entity} />
                    </span>
                ))}
            </HorizontalList>
        </Container>
    );
};
