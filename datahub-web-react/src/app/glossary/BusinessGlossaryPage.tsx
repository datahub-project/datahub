import React, { useState } from 'react';
import { Button, Typography } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';

import { useGetRootGlossaryNodesQuery, useGetRootGlossaryTermsQuery } from '../../graphql/glossary.generated';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import GlossaryEntitiesPath from './GlossaryEntitiesPath';
import GlossaryEntitiesList from './GlossaryEntitiesList';
import GlossaryBrowser from './GlossaryBrowser/GlossaryBrowser';
import GlossarySearch from './GlossarySearch';
import { ProfileSidebarResizer } from '../entity/shared/containers/profile/sidebar/ProfileSidebarResizer';
import EmptyGlossarySection from './EmptyGlossarySection';
import CreateGlossaryEntityModal from '../entity/shared/EntityDropdown/CreateGlossaryEntityModal';
import { EntityType } from '../../types.generated';
import { Message } from '../shared/Message';

export const HeaderWrapper = styled(TabToolbar)`
    padding: 15px 45px 10px 24px;
    height: auto;
`;

const GlossaryWrapper = styled.div`
    display: flex;
    flex: 1;
    max-height: inherit;
`;

const MainContentWrapper = styled.div`
    display: flex;
    flex: 1;
    flex-direction: column;
`;

export const BrowserWrapper = styled.div<{ width: number }>`
    max-height: 100%;
    min-width: ${(props) => props.width}px;
`;

export const MAX_BROWSER_WIDTH = 500;
export const MIN_BROWSWER_WIDTH = 200;

function BusinessGlossaryPage() {
    const [browserWidth, setBrowserWidth] = useState(window.innerWidth * 0.2);
    const { data: termsData, refetch: refetchForTerms, loading: termsLoading } = useGetRootGlossaryTermsQuery();
    const { data: nodesData, refetch: refetchForNodes, loading: nodesLoading } = useGetRootGlossaryNodesQuery();

    const terms = termsData?.getRootGlossaryTerms?.terms;
    const nodes = nodesData?.getRootGlossaryNodes?.nodes;

    const hasTermsOrNodes = !!nodes?.length || !!terms?.length;

    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);

    return (
        <>
            <GlossaryWrapper>
                {(termsLoading || nodesLoading) && (
                    <Message type="loading" content="Loading Glossary..." style={{ marginTop: '10%' }} />
                )}
                <BrowserWrapper width={browserWidth}>
                    <GlossarySearch />
                    <GlossaryBrowser rootNodes={nodes} rootTerms={terms} />
                </BrowserWrapper>
                <ProfileSidebarResizer
                    setSidePanelWidth={(width) =>
                        setBrowserWidth(Math.min(Math.max(width, MIN_BROWSWER_WIDTH), MAX_BROWSER_WIDTH))
                    }
                    initialSize={browserWidth}
                    isSidebarOnLeft
                />
                <MainContentWrapper>
                    <GlossaryEntitiesPath />
                    <HeaderWrapper>
                        <Typography.Title level={3}>Glossary</Typography.Title>
                        <div>
                            <Button type="text" onClick={() => setIsCreateTermModalVisible(true)}>
                                <PlusOutlined /> Add Term
                            </Button>
                            <Button type="text" onClick={() => setIsCreateNodeModalVisible(true)}>
                                <PlusOutlined /> Add Term Group
                            </Button>
                        </div>
                    </HeaderWrapper>
                    {hasTermsOrNodes && <GlossaryEntitiesList nodes={nodes || []} terms={terms || []} />}
                    {!(termsLoading || nodesLoading) && !hasTermsOrNodes && (
                        <EmptyGlossarySection refetchForTerms={refetchForTerms} refetchForNodes={refetchForNodes} />
                    )}
                </MainContentWrapper>
            </GlossaryWrapper>
            {isCreateTermModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryTerm}
                    onClose={() => setIsCreateTermModalVisible(false)}
                    refetchData={refetchForTerms}
                />
            )}
            {isCreateNodeModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryNode}
                    onClose={() => setIsCreateNodeModalVisible(false)}
                    refetchData={refetchForNodes}
                />
            )}
        </>
    );
}

export default BusinessGlossaryPage;
