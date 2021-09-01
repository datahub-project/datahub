import * as React from 'react';

import { Col, Row, Divider, Layout, Card, Typography } from 'antd';
import { LayoutProps } from 'antd/lib/layout';
import styled from 'styled-components';
import { TagOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';

import { RoutedTabs } from './RoutedTabs';
import CompactContext from './CompactContext';

export interface EntityProfileProps {
    title: string;
    tags?: React.ReactNode;
    tagCardHeader?: string;
    header: React.ReactNode;
    tabs?: Array<{
        name: string;
        path: string;
        content: React.ReactNode;
    }>;
    titleLink?: string;
    onTabChange?: (selectedTab: string) => void;
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

type LayoutPropsExtended = {
    isCompact: boolean;
};

const LayoutContent = styled(({ isCompact: _, ...props }: LayoutProps & LayoutPropsExtended) => (
    <Layout.Content {...props} />
))`
    padding: 0px ${(props) => (props.isCompact ? '0px' : '100px')};
`;

const LayoutDiv = styled(({ isCompact: _, ...props }: LayoutProps & LayoutPropsExtended) => (
    <Layout.Content {...props} />
))`
    padding-right: ${(props) => (props.isCompact ? '0px' : '24px')};
`;

const defaultProps = {
    tags: null,
    tabs: [],
    tagCardHeader: 'Tags',
};

/**
 * A default container view for presenting Entity details.
 */
export const LegacyEntityProfile = ({
    title,
    tags,
    header,
    tabs,
    titleLink,
    onTabChange,
    tagCardHeader,
}: EntityProfileProps) => {
    const isCompact = React.useContext(CompactContext);
    const defaultTabPath = tabs && tabs?.length > 0 ? tabs[0].path : '';
    /* eslint-disable spaced-comment */
    return (
        <LayoutContent isCompact={isCompact}>
            <Row>
                <Col md={isCompact ? 24 : 16} sm={24} xs={24}>
                    <LayoutDiv isCompact={isCompact}>
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
                    </LayoutDiv>
                </Col>
                {!!tags && (
                    <Col md={isCompact ? 24 : 8} xs={24} sm={24}>
                        <TagCard>
                            <TagsTitle type="secondary" level={4}>
                                <TagIcon /> {tagCardHeader}
                            </TagsTitle>
                            {tags}
                        </TagCard>
                    </Col>
                )}
            </Row>
            {!isCompact && (
                <>
                    <Divider style={{ marginBottom: '0px' }} />
                    <Row style={{ padding: '0px 0px 10px 0px' }}>
                        <Col span={24}>
                            <RoutedTabs defaultPath={defaultTabPath} tabs={tabs || []} onTabChange={onTabChange} />
                        </Col>
                    </Row>
                </>
            )}
        </LayoutContent>
    );
};

LegacyEntityProfile.defaultProps = defaultProps;
