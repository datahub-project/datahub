import React from 'react';
import 'antd/dist/antd.css';
import { Card, Typography, Row } from 'antd';
import { Link } from 'react-router-dom';
import '../../App.css';
import { useEntityRegistry } from '../useEntityRegistry';
import { PageRoutes } from '../../conf/Global';
import { IconStyleType } from '../entity/Entity';
import { EntityType } from '../../types.generated';

const styles = {
    card: { width: 360 },
    title: { margin: 0, color: '#525252' },
    iconFlag: { right: '32px', top: '-28px' },
    icon: { padding: '16px 24px' },
};

export const BrowseEntityCard = ({ entityType }: { entityType: EntityType }) => {
    const entityRegistry = useEntityRegistry();
    return (
        <Link to={`${PageRoutes.BROWSE}/${entityRegistry.getPathName(entityType)}`}>
            <Card hoverable>
                <Row justify="space-between" align="middle" style={styles.card}>
                    <Typography.Title style={styles.title} level={4}>
                        {entityRegistry.getCollectionName(entityType)}
                    </Typography.Title>
                    <Card bodyStyle={styles.icon} style={{ ...styles.iconFlag, position: 'absolute' }}>
                        {entityRegistry.getIcon(entityType, 24, IconStyleType.HIGHLIGHT)}
                    </Card>
                </Row>
            </Card>
        </Link>
    );
};
