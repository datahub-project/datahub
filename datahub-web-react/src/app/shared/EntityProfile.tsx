import * as React from 'react';
import { Col, Row, Divider, Layout, Space } from 'antd';
import styled from 'styled-components';

import { RoutedTabs } from './RoutedTabs';
import { GlobalTags } from '../../types.generated';
import TagGroup from './TagGroup';

export interface EntityProfileProps {
    title: string;
    tags?: GlobalTags;
    header: React.ReactNode;
    tabs?: Array<{
        name: string;
        path: string;
        content: React.ReactNode;
    }>;
}

const TagsContainer = styled.div`
    margin-top: -8px;
`;

const defaultProps = {
    tags: [],
    tabs: [],
};

/**
 * A default container view for presenting Entity details.
 */
export const EntityProfile = ({ title, tags, header, tabs }: EntityProfileProps) => {
    const defaultTabPath = tabs && tabs?.length > 0 ? tabs[0].path : '';

    /* eslint-disable spaced-comment */
    return (
        <Layout.Content style={{ backgroundColor: 'white', padding: '0px 100px' }}>
            <Row style={{ padding: '20px 0px 10px 0px' }}>
                <Col span={24}>
                    <Space>
                        <h1>{title}</h1>
                        <TagsContainer>
                            <TagGroup globalTags={tags} />
                        </TagsContainer>
                    </Space>
                </Col>
            </Row>
            {header}
            <Divider style={{ marginBottom: '0px' }} />
            <Row style={{ padding: '0px 0px 10px 0px' }}>
                <Col span={24}>
                    <RoutedTabs defaultPath={defaultTabPath} tabs={tabs || []} />
                </Col>
            </Row>
        </Layout.Content>
    );
};

EntityProfile.defaultProps = defaultProps;
