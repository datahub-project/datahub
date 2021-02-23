import React from 'react';
import { Typography, Row, Col } from 'antd';
import { useEntityRegistry } from '../useEntityRegistry';
import { BrowseEntityCard } from '../search/BrowseEntityCard';

const styles = {
    title: {
        margin: '0px 0px 0px 120px',
        fontSize: 32,
    },
    entityGrid: {
        padding: '40px 100px',
    },
};

export const HomePageBody = () => {
    const entityRegistry = useEntityRegistry();
    return (
        <>
            <Typography.Text style={styles.title}>
                <b>Explore</b> your data
            </Typography.Text>
            <Row gutter={[16, 16]} style={styles.entityGrid}>
                {entityRegistry.getBrowseEntityTypes().map((entityType) => (
                    <Col xs={24} sm={24} md={8}>
                        <BrowseEntityCard entityType={entityType} />
                    </Col>
                ))}
            </Row>
        </>
    );
};
