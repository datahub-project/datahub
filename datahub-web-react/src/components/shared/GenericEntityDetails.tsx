import * as React from 'react';
import { Col, Row, Tag, Divider, Layout } from 'antd';
import { RoutedTabs } from './RoutedTabs';

interface Props {
    title: string;
    tags?: Array<string>;
    body: React.ReactNode;
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
 * A generic container view for presenting Entity details.
 */
export const GenericEntityDetails = ({ title: _title, tags: _tags, body: _body, tabs: _tabs }: Props) => {
    const defaultTabPath = _tabs && _tabs?.length > 0 ? _tabs[0].path : '';

    /* eslint-disable spaced-comment */
    return (
        <Layout.Content style={{ backgroundColor: 'white', padding: '0px 100px' }}>
            <Row style={{ padding: '20px 0px 10px 0px' }}>
                <Col span={24}>
                    <div>
                        <h1 style={{ float: 'left' }}>{_title}</h1>
                        <div style={{ float: 'left', margin: '5px 20px' }}>
                            {_tags && _tags.map((t) => <Tag color="blue">{t}</Tag>)}
                        </div>
                    </div>
                </Col>
            </Row>
            {_body}
            <Divider style={{ marginBottom: '0px' }} />
            <Row style={{ padding: '0px 0px 10px 0px' }}>
                <Col span={24}>
                    <RoutedTabs defaultPath={defaultTabPath} tabs={_tabs || []} />
                </Col>
            </Row>
        </Layout.Content>
    );
};

GenericEntityDetails.defaultProps = defaultProps;
