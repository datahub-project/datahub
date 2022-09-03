import React from 'react';
import { Col, Row } from 'antd';
import styled from 'styled-components';
import { useGetGroupQuery } from '../../../graphql/group.generated';
import useUserParams from '../../shared/entitySearch/routingUtils/useUserParams';
import { OriginType, EntityRelationshipsResult, Ownership } from '../../../types.generated';
import { Message } from '../../shared/Message';
import GroupMembers from './GroupMembers';
import { decodeUrn } from '../shared/utils';
import { RoutedTabs } from '../../shared/RoutedTabs';
import GroupInfoSidebar from './GroupInfoSideBar';
import { GroupAssets } from './GroupAssets';
import { ErrorSection } from '../../shared/error/ErrorSection';

const messageStyle = { marginTop: '10%' };

export enum TabType {
    Assets = 'Assets',
    Members = 'Members',
}

const ENABLED_TAB_TYPES = [TabType.Assets, TabType.Members];

const MEMBER_PAGE_SIZE = 15;

/**
 * Styled Components
 */
const GroupProfileWrapper = styled.div`
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
 * Responsible for reading & writing groups.
 */
export default function GroupProfile() {
    const { urn: encodedUrn } = useUserParams();
    const urn = encodedUrn && decodeUrn(encodedUrn);
    const { loading, error, data, refetch } = useGetGroupQuery({ variables: { urn, membersCount: MEMBER_PAGE_SIZE } });

    const groupMemberRelationships = data?.corpGroup?.relationships as EntityRelationshipsResult;
    const isExternalGroup: boolean = data?.corpGroup?.origin?.type === OriginType.External;
    const externalGroupType: string = data?.corpGroup?.origin?.externalType || 'outside DataHub';

    const getTabs = () => {
        return [
            {
                name: TabType.Assets,
                path: TabType.Assets.toLocaleLowerCase(),
                content: <GroupAssets urn={urn} />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: TabType.Members,
                path: TabType.Members.toLocaleLowerCase(),
                content: (
                    <GroupMembers
                        urn={urn}
                        pageSize={MEMBER_PAGE_SIZE}
                        isExternalGroup={isExternalGroup}
                        onChangeMembers={() => {
                            setTimeout(() => refetch(), 2000);
                        }}
                    />
                ),
                display: {
                    enabled: () => true,
                },
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';
    const onTabChange = () => null;

    // Side bar data
    const sideBarData = {
        photoUrl: undefined,
        avatarName:
            data?.corpGroup?.properties?.displayName ||
            data?.corpGroup?.name ||
            data?.corpGroup?.info?.displayName ||
            undefined,
        name:
            data?.corpGroup?.properties?.displayName ||
            data?.corpGroup?.name ||
            data?.corpGroup?.info?.displayName ||
            undefined,
        email: data?.corpGroup?.editableProperties?.email || data?.corpGroup?.properties?.email || undefined,
        slack: data?.corpGroup?.editableProperties?.slack || data?.corpGroup?.properties?.slack || undefined,
        aboutText:
            data?.corpGroup?.editableProperties?.description || data?.corpGroup?.properties?.description || undefined,
        groupMemberRelationships: groupMemberRelationships as EntityRelationshipsResult,
        groupOwnerShip: data?.corpGroup?.ownership as Ownership,
        isExternalGroup,
        externalGroupType,
        urn,
    };

    return (
        <>
            {error && <ErrorSection />}
            {loading && <Message type="loading" content="Loading..." style={messageStyle} />}
            {data && data?.corpGroup && (
                <GroupProfileWrapper>
                    <Row>
                        <Col xl={5} lg={5} md={5} sm={24} xs={24}>
                            <GroupInfoSidebar sideBarData={sideBarData} refetch={refetch} />
                        </Col>
                        <Col xl={19} lg={19} md={19} sm={24} xs={24} style={{ borderLeft: '1px solid #E9E9E9' }}>
                            <Content>
                                <RoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
                            </Content>
                        </Col>
                    </Row>
                </GroupProfileWrapper>
            )}
        </>
    );
}
