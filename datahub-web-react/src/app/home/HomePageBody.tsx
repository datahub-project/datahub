import React from 'react';
import { Typography, Row, Col } from 'antd';
import styled from 'styled-components';

import { useEntityRegistry } from '../useEntityRegistry';
import { BrowseEntityCard } from '../search/BrowseEntityCard';
import { useGetEntityCountsQuery } from '../../graphql/app.generated';
import { EntityType } from '../../types.generated';

const Title = styled(Typography.Text)`
    && {
        margin: 0px 0px 0px 120px;
        font-size: 32px;
        color: ${(props) =>
            props.theme.styles['homepage-text-color'] || props.theme.styles['homepage-background-upper-fade']};
    }
`;

const EntityGridRow = styled(Row)`
    padding: 40px 100px;
    margin: 0 !important;
`;

const BodyContainer = styled.div`
    background-color: ${(props) => props.theme.styles['homepage-background-lower-fade']};
`;

// TODO: Make this list config-driven
const PERMANENT_ENTITY_TYPES = [EntityType.Dataset, EntityType.Chart, EntityType.Dashboard];

export const HomePageBody = () => {
    const entityRegistry = useEntityRegistry();
    const browseEntityList = entityRegistry.getBrowseEntityTypes();

    const { data: entityCountData } = useGetEntityCountsQuery({
        variables: {
            input: {
                types: browseEntityList,
            },
        },
    });

    const orderedEntityCounts =
        entityCountData?.getEntityCounts?.counts?.sort((a, b) => {
            return browseEntityList.indexOf(a.entityType) - browseEntityList.indexOf(b.entityType);
        }) || PERMANENT_ENTITY_TYPES.map((entityType) => ({ count: 0, entityType }));

    return (
        <BodyContainer>
            <Title>
                <b>Explore</b> your data
            </Title>
            <EntityGridRow gutter={[16, 24]}>
                {orderedEntityCounts.map(
                    (entityCount) =>
                        entityCount &&
                        (entityCount.count !== 0 || PERMANENT_ENTITY_TYPES.indexOf(entityCount.entityType) !== -1) && (
                            <Col xs={24} sm={24} md={8} key={entityCount.entityType}>
                                <BrowseEntityCard entityType={entityCount.entityType} />
                            </Col>
                        ),
                )}
            </EntityGridRow>
        </BodyContainer>
    );
};
