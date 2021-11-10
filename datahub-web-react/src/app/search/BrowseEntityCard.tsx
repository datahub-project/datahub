import React from 'react';
import { Card, Typography, Row } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { useEntityRegistry } from '../useEntityRegistry';
import { PageRoutes } from '../../conf/Global';
import { IconStyleType } from '../entity/Entity';
import { EntityType } from '../../types.generated';

const styles = {
    row: { width: 360 },
    title: { margin: 0, fontWeight: 500 },
    iconFlag: { right: '32px', top: '-28px' },
    icon: { padding: '16px 24px' },
};

const EntityCard = styled(Card)`
    && {
        margin-top: 16px;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        box-shadow: ${(props) => props.theme.styles['box-shadow']};
    }
    &&:hover {
        box-shadow: ${(props) => props.theme.styles['box-shadow-hover']};
    }
`;

const FlagCard = styled(Card)`
    &&& {
        right: 32px;
        top: -28px;
        position: absolute;
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }
`;

export const BrowseEntityCard = ({ entityType }: { entityType: EntityType }) => {
    const entityRegistry = useEntityRegistry();
    return (
        <Link to={`${PageRoutes.BROWSE}/${entityRegistry.getPathName(entityType)}`}>
            <EntityCard hoverable>
                <Row justify="space-between" align="middle" style={styles.row}>
                    <Typography.Title style={styles.title} level={4}>
                        {entityRegistry.getCollectionName(entityType)}
                    </Typography.Title>
                    <FlagCard bodyStyle={styles.icon}>
                        {entityRegistry.getIcon(entityType, 24, IconStyleType.HIGHLIGHT)}
                    </FlagCard>
                </Row>
            </EntityCard>
        </Link>
    );
};
