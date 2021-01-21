import * as React from 'react';
import 'antd/dist/antd.css';
import { Link } from 'react-router-dom';
import { Col, Row, Card } from 'antd';
import { Content } from 'antd/lib/layout/layout';
import { BrowseCfg } from '../../conf';
import { SearchablePage } from '../search/SearchablePage';
import { toCollectionName, toPathName } from '../shared/EntityTypeUtil';
import { PageRoutes } from '../../conf/Global';
import '../../App.css';

const { BROWSABLE_ENTITY_TYPES } = BrowseCfg;

export const BrowseTypesPage = () => {
    return (
        <SearchablePage>
            <Content style={{ backgroundColor: 'white', padding: '25px 100px' }}>
                <h1 className="ant-typography">Browse</h1>
                <Row gutter={[16, 16]}>
                    {BROWSABLE_ENTITY_TYPES.map((entityType) => (
                        <Col xs={24} sm={24} md={8}>
                            <Link to={`${PageRoutes.BROWSE}/${toPathName(entityType)}`}>
                                <Card
                                    style={{
                                        padding: '30px 0px',
                                        display: 'flex',
                                        justifyContent: 'center',
                                    }}
                                    hoverable
                                >
                                    <div style={{ fontSize: '18px', fontWeight: 'bold', color: '#0073b1' }}>
                                        {toCollectionName(entityType)}
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
