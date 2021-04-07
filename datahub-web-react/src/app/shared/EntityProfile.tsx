import * as React from 'react';

import { Col, Row, Divider, Layout, Card, Typography } from 'antd';
import styled from 'styled-components';
import { TagOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';

import { RoutedTabs } from './RoutedTabs';
import CompactContext from './CompactContext';

export interface EntityProfileProps {
    title: string;
    tags?: React.ReactNode;
    header: React.ReactNode;
    tabs?: Array<{
        name: string;
        path: string;
        content: React.ReactNode;
    }>;
    titleLink?: string;
}

const TagsTitle = styled(Typography.Title)`
    font-size: 18px;
`;

const TagCard = styled(Card)`
    margin-top: 24px;
    font-size: 18px;
    min-width: 100%;
    width: 100%;
`;

const TagIcon = styled(TagOutlined)`
    padding-right: 6px;
`;

type LayoutProps = {
    isCompact: boolean;
};

const LayoutContent = styled(Layout.Content)<LayoutProps>`
    padding: 0px ${(props) => (props.isCompact ? '0px' : '100px')};
`;

const defaultProps = {
    tags: [],
    tabs: [],
};

/**
 * A default container view for presenting Entity details.
 */
export const EntityProfile = ({ title, tags, header, tabs, titleLink }: EntityProfileProps) => {
    const isCompact = React.useContext(CompactContext);
    const defaultTabPath = tabs && tabs?.length > 0 ? tabs[0].path : '';

    /* eslint-disable spaced-comment */
    return (
        <LayoutContent isCompact={isCompact}>
            <div>
                <Row>
                    <Col md={isCompact ? 24 : 16} sm={24} xs={24}>
                        <div>
                            <Row style={{ padding: '20px 0px 10px 0px' }}>
                                <Col span={24}>
                                    {titleLink ? (
                                        <Link to={titleLink}>
                                            <h1>{title}</h1>
                                        </Link>
                                    ) : (
                                        <h1>{title}</h1>
                                    )}
                                </Col>
                            </Row>
                            {header}
                        </div>
                    </Col>
                    <Col md={isCompact ? 24 : 8} xs={24} sm={24}>
                        <TagCard>
                            <TagsTitle type="secondary" level={4}>
                                <TagIcon /> Tags
                            </TagsTitle>
                            {tags}
                        </TagCard>
                    </Col>
                </Row>
                {!isCompact && (
                    <>
                        <Divider style={{ marginBottom: '0px' }} />
                        <Row style={{ padding: '0px 0px 10px 0px' }}>
                            <Col span={24}>
                                <RoutedTabs defaultPath={defaultTabPath} tabs={tabs || []} />
                            </Col>
                        </Row>
                    </>
                )}
            </div>
        </LayoutContent>
    );
};

EntityProfile.defaultProps = defaultProps;
