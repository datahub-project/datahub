import * as React from 'react';
import { Col, Row, Tag, Divider, Layout } from 'antd';
import { RoutedTabs } from './RoutedTabs';

export interface EntityProfileProps {
    title: string;
    tags?: Array<string>;
    header: React.ReactNode;
    tabs?: Array<{
        name: string;
        path: string;
        content: React.ReactNode;
    }>;
}

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
                    <div>
                        <h1 style={{ float: 'left' }}>{title}</h1>
                        <div style={{ float: 'left', margin: '5px 20px' }}>
                            {tags && tags.map((t) => <Tag color="blue">{t}</Tag>)}
                        </div>
                    </div>
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
