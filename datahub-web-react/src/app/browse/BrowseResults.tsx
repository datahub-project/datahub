import React from 'react';
import styled from 'styled-components';
import { Col, Divider, List, Pagination, Row, Empty } from 'antd';
import { Content } from 'antd/lib/layout/layout';
import { BrowseResultGroup, EntityType, Entity } from '../../types.generated';
import BrowseResultCard from './BrowseResultCard';
import { useEntityRegistry } from '../useEntityRegistry';
import analytics, { EventType } from '../analytics';

const EntityList = styled(List)`
    && {
        width: 100%;
        margin-top: 12px;
        padding: 16px 32px;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        box-shadow: ${(props) => props.theme.styles['box-shadow']};
    }
`;

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

    const onGroupClick = (group: BrowseResultGroup) => {
        analytics.event({
            type: EventType.BrowseResultClickEvent,
            browsePath: rootPath,
            entityType: type,
            resultType: 'Group',
            groupName: group.name,
        });
    };

    const onEntityClick = (entity: Entity) => {
        analytics.event({
            type: EventType.BrowseResultClickEvent,
            browsePath: rootPath,
            entityType: type,
            resultType: 'Entity',
            entityUrn: entity.urn,
        });
    };

    return (
        <div>
            <Content style={{ padding: '25px 100px' }}>
                <h1 className="ant-typography">{title}</h1>
                <Row gutter={[4, 8]}>
                    {groups.map((group) => (
                        <Col span={24} key={`${group.name}_key`}>
                            <BrowseResultCard
                                onClick={() => onGroupClick(group)}
                                name={group.name}
                                count={group.count}
                                url={`${rootPath}/${group.name}`}
                                type={entityRegistry.getCollectionName(type)}
                            />
                        </Col>
                    ))}
                    {(!(groups && groups.length > 0) || (entities && entities.length > 0)) && (
                        <EntityList
                            dataSource={entities}
                            split={false}
                            renderItem={(item, index) => (
                                <>
                                    <List.Item onClick={() => onEntityClick(item as Entity)}>
                                        {entityRegistry.renderBrowse(type, item)}
                                    </List.Item>
                                    {index < entities.length - 1 && <Divider />}
                                </>
                            )}
                            bordered
                            locale={{
                                emptyText: <Empty description="No Entities" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                            }}
                        />
                    )}
                    <Col span={24}>
                        <Pagination
                            style={{ width: '100%', display: 'flex', justifyContent: 'center', paddingTop: 16 }}
                            current={page}
                            pageSize={pageSize}
                            total={totalResults}
                            showTitle
                            showLessItems
                            onChange={onChangePage}
                            showSizeChanger={false}
                        />
                    </Col>
                </Row>
            </Content>
        </div>
    );
};
