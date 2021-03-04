import React from 'react';
import { Col, Divider, Pagination, Row } from 'antd';
import { Content } from 'antd/lib/layout/layout';
import { BrowseResultGroup, EntityType, Entity } from '../../types.generated';
import BrowseResultCard from './BrowseResultCard';
import { useEntityRegistry } from '../useEntityRegistry';

const styles = {
    browseCard: { marginTop: 20 },
};

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
    return (
        <div>
            <Content style={{ backgroundColor: 'white', padding: '25px 100px' }}>
                <h1 className="ant-typography">{title}</h1>
                <Row gutter={[4, 8]}>
                    {groups.map((group) => (
                        <Col span={24}>
                            <BrowseResultCard name={group.name} count={group.count} url={`${rootPath}/${group.name}`} />
                        </Col>
                    ))}
                    {entities.map((entity) => (
                        <Col style={styles.browseCard} span={24}>
                            {entityRegistry.renderBrowse(type, entity)}
                            <Divider />
                        </Col>
                    ))}
                    <Col span={24}>
                        <Pagination
                            style={{ width: '100%', display: 'flex', justifyContent: 'center', paddingTop: 16 }}
                            current={page}
                            pageSize={pageSize}
                            total={totalResults}
                            showTitle
                            showLessItems
                            onChange={onChangePage}
                        />
                    </Col>
                </Row>
            </Content>
        </div>
    );
};
