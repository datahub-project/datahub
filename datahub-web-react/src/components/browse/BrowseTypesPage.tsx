import React from 'react';
import 'antd/dist/antd.css';
import { Link } from 'react-router-dom';
import { Col, Row, Card } from 'antd';
import { Content } from 'antd/lib/layout/layout';
import { SearchablePage } from '../search/SearchablePage';
import { PageRoutes } from '../../conf/Global';
import '../../App.css';
import { useEntityRegistry } from '../useEntityRegistry';

export const BrowseTypesPage = () => {
    const entityRegistry = useEntityRegistry();
    return (
        <SearchablePage>
            <Content style={{ backgroundColor: 'white', padding: '25px 100px' }}>
                <h1 className="ant-typography">Browse</h1>
                <Row gutter={[16, 16]}>
                    {entityRegistry.getBrowseEntityTypes().map((entityType) => (
                        <Col xs={24} sm={24} md={8}>
                            <Link to={`${PageRoutes.BROWSE}/${entityRegistry.getPathName(entityType)}`}>
                                <Card
                                    style={{
                                        padding: '30px 0px',
                                        display: 'flex',
                                        justifyContent: 'center',
                                    }}
                                    hoverable
                                >
                                    <div style={{ fontSize: '18px', fontWeight: 'bold', color: '#0073b1' }}>
                                        {entityRegistry.getCollectionName(entityType)}
                                    </div>
                                </Card>
                            </Link>
                        </Col>
                    ))}
                </Row>
            </Content>
        </SearchablePage>
    );
};
