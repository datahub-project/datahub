import { Alert, Col, Row } from 'antd';
import React, { useMemo } from 'react';
// import React from 'react';
import styled from 'styled-components';
// import GroupHeader from './GroupHeader';
import { useGetGroupQuery } from '../../../graphql/group.generated';
import { useGetAllEntitySearchResults } from '../../../utils/customGraphQL/useGetAllEntitySearchResults';
import useUserParams from '../../shared/entitySearch/routingUtils/useUserParams';
import { EntityRelationshipsResult, EntityType, SearchResult } from '../../../types.generated';
// import { EntityRelationshipsResult } from '../../../types.generated';
// import RelatedEntityResults from '../../shared/entitySearch/RelatedEntityResults';
import { Message } from '../../shared/Message';
import GroupMembers from './GroupMembers';
// import { LegacyEntityProfile } from '../../shared/LegacyEntityProfile';
// import { useEntityRegistry } from '../../useEntityRegistry';
import { decodeUrn } from '../shared/utils';
import { RoutedTabs } from '../../shared/RoutedTabs';
import GroupInfoSidebar from './GroupInfoSideBar';
import { GroupAssets } from './GroupAssets';

const messageStyle = { marginTop: '10%' };

// export enum TabType {
//     Members = 'Members',
//     Ownership = 'Ownership',
// }

export enum TabType {
    Assets = 'Assets',
    Members = 'Members',
}

const ENABLED_TAB_TYPES = [TabType.Assets, TabType.Members];

const MEMBER_PAGE_SIZE = 20;

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
    // const entityRegistry = useEntityRegistry();
    const { urn: encodedUrn } = useUserParams();
    const urn = encodedUrn && decodeUrn(encodedUrn);
    const { loading, error, data, refetch } = useGetGroupQuery({ variables: { urn, membersCount: MEMBER_PAGE_SIZE } });

    const ownershipResult = useGetAllEntitySearchResults({
        query: `owners:${data?.corpGroup?.name}`,
    });

    const contentLoading =
        Object.keys(ownershipResult).some((type) => {
            return ownershipResult[type].loading;
        }) || loading;

    const ownershipForDetails = useMemo(() => {
        const filteredOwnershipResult: {
            [key in EntityType]?: Array<SearchResult>;
        } = {};

        Object.keys(ownershipResult).forEach((type) => {
            const entities = ownershipResult[type].data?.search?.searchResults;

            if (entities && entities.length > 0) {
                filteredOwnershipResult[type] = ownershipResult[type].data?.search?.searchResults;
            }
        });
        return filteredOwnershipResult;
    }, [ownershipResult]);

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Group failed to load :('} />;
    }

    const groupMemberRelationships = data?.corpGroup?.relationships as EntityRelationshipsResult;

    const getTabs = () => {
        return [
            {
                name: TabType.Members,
                path: TabType.Members.toLocaleLowerCase(),
                content: (
                    <GroupMembers
                        urn={urn}
                        initialRelationships={groupMemberRelationships}
                        pageSize={MEMBER_PAGE_SIZE}
                    />
                ),
                display: {
                    enabled: () => true,
                },
            },
            {
                name: TabType.Assets,
                path: TabType.Assets.toLocaleLowerCase(),
                content: <GroupAssets urn={urn} />,
                display: {
                    enabled: () => true,
                },
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';
    const onTabChange = () => null;
    // const description = data?.corpGroup?.info?.description;

    // Side bar data
    const sideBarData = {
        photoUrl: undefined,
        avatarName: data?.corpGroup?.info?.displayName || data?.corpGroup?.name,
        name: data?.corpGroup?.info?.displayName || data?.corpGroup?.name || undefined,
        role: 'data?.corpGroup' || undefined,
        team: 'data?.corpGroup' || undefined,
        email: data?.corpGroup?.editableProperties?.email || undefined,
        slack: data?.corpGroup?.editableProperties?.slack || undefined,
        phone: 'data?.corpGroup' || undefined,
        aboutText: data?.corpGroup?.info?.description || undefined,
        groupMemberRelationships: groupMemberRelationships as EntityRelationshipsResult,
        urn,
    };
    console.log('data', data, 'group member', groupMemberRelationships, 'owner', ownershipForDetails);
    return (
        <>
            {contentLoading && <Message type="loading" content="Loading..." style={messageStyle} />}
            {data && data?.corpGroup && (
                // <LegacyEntityProfile
                //     title=""
                //     tags={null}
                //     header={
                //         <GroupHeader
                //             name={entityRegistry.getDisplayName(EntityType.CorpGroup, data?.corpGroup)}
                //             description={description}
                //             email={data?.corpGroup?.info?.email}
                //         />
                //     }
                //     tabs={getTabs()}
                // />
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
