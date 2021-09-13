import React from 'react';
import { Typography, Row, Col } from 'antd';
import styled from 'styled-components';

import { useEntityRegistry } from '../useEntityRegistry';
import { BrowseEntityCard } from '../search/BrowseEntityCard';

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

export const HomePageBody = () => {
    const entityRegistry = useEntityRegistry();

    return (
        <BodyContainer>
            <Title>
                <b>Explore</b> your data
            </Title>
            <EntityGridRow gutter={[16, 24]}>
                {entityRegistry.getBrowseEntityTypes().map((entityType) => (
                    <Col xs={24} sm={24} md={8} key={entityType}>
                        <BrowseEntityCard entityType={entityType} />
                    </Col>
                ))}
            </EntityGridRow>
        </BodyContainer>
    );
};
