import React from 'react';
import { Typography, Row, Col } from 'antd';
import styled from 'styled-components';

import { useEntityRegistry } from '../useEntityRegistry';
import { BrowseEntityCard } from '../search/BrowseEntityCard';

import themeConfig from '../../theme.config.json';

const Title = styled(Typography.Text)`
    && {
        margin: 0px 0px 0px 120px;
        font-size: 32px;
        color: ${themeConfig.appVariables.homepage.backgroundColorUpperFade};
    }
`;

const EntityGridRow = styled(Row)`
    padding: 40px 100px;
`;

const BodyContainer = styled.div`
    background-color: ${themeConfig.appVariables.homepage.backgroundColorLowerFade};
`;

export const HomePageBody = () => {
    const entityRegistry = useEntityRegistry();
    return (
        <BodyContainer>
            <Title>
                <b>Explore</b> your data
            </Title>
            <EntityGridRow gutter={[16, 16]}>
                {entityRegistry.getBrowseEntityTypes().map((entityType) => (
                    <Col xs={24} sm={24} md={8}>
                        <BrowseEntityCard entityType={entityType} />
                    </Col>
                ))}
            </EntityGridRow>
        </BodyContainer>
    );
};
