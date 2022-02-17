import { Alert, Col, Row } from 'antd';
import React from 'react';
import styled from 'styled-components';
import useUserParams from '../../shared/entitySearch/routingUtils/useUserParams';
import { useGetUserQuery } from '../../../graphql/user.generated';
import { useGetAllEntitySearchResults } from '../../../utils/customGraphQL/useGetAllEntitySearchResults';
import { Message } from '../../shared/Message';
import { EntityRelationshipsResult } from '../../../types.generated';
import UserGroups from './UserGroups';
import { RoutedTabs } from '../../shared/RoutedTabs';
import { UserAssets } from './UserAssets';
import { ExtendedEntityRelationshipsResult } from './type';
import { decodeUrn } from '../shared/utils';
import UserInfoSideBar from './UserInfoSideBar';

export interface Props {
    onTabChange: (selectedTab: string) => void;
}

export enum TabType {
    Assets = 'Assets',
    Groups = 'Groups',
}
const ENABLED_TAB_TYPES = [TabType.Assets, TabType.Groups];

const GROUP_PAGE_SIZE = 20;
const MESSAGE_STYLE = { marginTop: '10%' };

/**
 * Styled Components
 */
const UserProfileWrapper = styled.div`
    &&& .ant-tabs-nav {
        margin: 0;
    }
`;

const Content = styled.div`
    color: #262626;
    height: calc(100vh - 60px);

    &&& .ant-tabs > .ant-tabs-nav .ant-tabs-nav-wrap {
        padding-left: 15px;
    }
`;

/**
 * Responsible for reading & writing users.
 */
export default function UserProfile() {
    const { urn: encodedUrn } = useUserParams();
    const urn = decodeUrn(encodedUrn);

    const { loading, error, data } = useGetUserQuery({ variables: { urn, groupsCount: GROUP_PAGE_SIZE } });

    const username = data?.corpUser?.username;
    const ownershipResult = useGetAllEntitySearchResults({
        query: `owners:${username}`,
    });

    const groupMemberRelationships = data?.corpUser?.relationships as EntityRelationshipsResult;
    const groupsDetails = data?.corpUser?.relationships as ExtendedEntityRelationshipsResult;

    const contentLoading =
        Object.keys(ownershipResult).some((type) => {
            return ownershipResult[type].loading;
        }) || loading;

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    // Routed Tabs Constants
    const getTabs = () => {
        return [
            {
                name: TabType.Assets,
                path: TabType.Assets.toLocaleLowerCase(),
                content: <UserAssets />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: TabType.Groups,
                path: TabType.Groups.toLocaleLowerCase(),
                content: (
                    <UserGroups urn={urn} initialRelationships={groupMemberRelationships} pageSize={GROUP_PAGE_SIZE} />
                ),
                display: {
                    enabled: () => groupsDetails?.relationships.length > 0,
                },
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };
    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';
    const onTabChange = () => null;

    return (
        <>
            {contentLoading && <Message type="loading" content="Loading..." style={MESSAGE_STYLE} />}
            <UserProfileWrapper>
                <Row>
                    <Col xl={5} lg={5} md={5} sm={24} xs={24}>
                        <UserInfoSideBar />
                    </Col>
                    <Col xl={19} lg={19} md={19} sm={24} xs={24} style={{ borderLeft: '1px solid #E9E9E9' }}>
                        <Content>
                            <RoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
                        </Content>
                    </Col>
                </Row>
            </UserProfileWrapper>
        </>
    );
}
