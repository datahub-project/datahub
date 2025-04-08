import { EditOutlined, ExpandAltOutlined, PlusOutlined } from '@ant-design/icons';
import { Button as AntButton, Divider, Typography } from 'antd';
import queryString from 'query-string';
import React, { useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityData, useRefetch, useRouteToTab } from '@app/entity/shared/EntityContext';
import { AddLinkModal } from '@app/entityV2/shared/components/styled/AddLinkModal';
import { EmptyTab } from '@app/entityV2/shared/components/styled/EmptyTab';
import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { DescriptionEditor } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionEditor';
import { DescriptionPreviewModal } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionPreviewModal';
import { LinkList } from '@app/entityV2/shared/tabs/Documentation/components/LinkList';
import { Editor } from '@app/entityV2/shared/tabs/Documentation/components/editor/Editor';
import { getAssetDescriptionDetails } from '@app/entityV2/shared/tabs/Documentation/utils';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '@app/entityV2/shared/utils';
import { Button } from '@src/alchemy-components';

const DocumentationContainer = styled.div`
    margin: 0 32px;
    padding: 40px 0;
    max-width: calc(100% - 10px);
`;

const StyledTabToolbar = styled(TabToolbar)`
    background-color: ${REDESIGN_COLORS.LIGHT_GREY};
    border-top-left-radius: 4px;
    border-bottom-left-radius: 4px;
    border-left: 2px solid #5c3fd1;
    padding: 8px 20px;
    margin: 2px 14px 2px 12px;

    position: sticky;
    top: 0;
`;

const EmptyTabWrapper = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    height: 100%;
`;

interface Props {
    hideLinksButton?: boolean;
}

export const DocumentationTab = ({ properties }: { properties?: Props }) => {
    const hideLinksButton = properties?.hideLinksButton;
    const { urn, entityData } = useEntityData();

    const refetch = useRefetch();
    const { displayedDescription } = getAssetDescriptionDetails({
        entityProperties: entityData,
    });
    const links = entityData?.institutionalMemory?.elements || [];
    const localStorageDictionary = localStorage.getItem(EDITED_DESCRIPTIONS_CACHE_NAME);

    const routeToTab = useRouteToTab();
    const isEditing = queryString.parse(useLocation().search, { parseBooleans: true }).editing;
    const showModal = queryString.parse(useLocation().search, { parseBooleans: true }).modal;

    useEffect(() => {
        const editedDescriptions = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};
        if (editedDescriptions.hasOwnProperty(urn)) {
            routeToTab({
                tabName: 'Documentation',
                tabParams: { editing: true, modal: !!showModal },
            });
        }
    }, [urn, routeToTab, showModal, localStorageDictionary]);

    return isEditing && !showModal ? (
        <DescriptionEditor onComplete={() => routeToTab({ tabName: 'Documentation' })} />
    ) : (
        <>
            {displayedDescription || links.length ? (
                <>
                    <StyledTabToolbar>
                        <div>
                            <AntButton
                                data-testid="edit-documentation-button"
                                type="text"
                                onClick={() => routeToTab({ tabName: 'Documentation', tabParams: { editing: true } })}
                            >
                                <EditOutlined /> Edit
                            </AntButton>
                            {!hideLinksButton && <AddLinkModal buttonType="text" refetch={refetch} />}
                        </div>
                        <div>
                            <AntButton
                                type="text"
                                onClick={() =>
                                    routeToTab({
                                        tabName: 'Documentation',
                                        tabParams: { modal: true },
                                    })
                                }
                            >
                                <ExpandAltOutlined />
                            </AntButton>
                        </div>
                    </StyledTabToolbar>
                    <div>
                        {displayedDescription ? (
                            [<Editor content={displayedDescription} readOnly />]
                        ) : (
                            <DocumentationContainer>
                                <Typography.Text type="secondary">No documentation added yet.</Typography.Text>
                            </DocumentationContainer>
                        )}
                        <Divider />
                        <DocumentationContainer>
                            {!hideLinksButton && <LinkList refetch={refetch} />}
                        </DocumentationContainer>
                    </div>
                </>
            ) : (
                <EmptyTabWrapper>
                    <EmptyTab tab="documentation" hideImage={false}>
                        {!hideLinksButton && <AddLinkModal refetch={refetch} />}
                        <Button
                            data-testid="add-documentation"
                            onClick={() => routeToTab({ tabName: 'Documentation', tabParams: { editing: true } })}
                        >
                            <PlusOutlined /> Add Documentation
                        </Button>
                    </EmptyTab>
                </EmptyTabWrapper>
            )}
            {showModal && (
                <DescriptionPreviewModal
                    editMode={(isEditing && true) || false}
                    description={displayedDescription}
                    onClose={() => {
                        routeToTab({ tabName: 'Documentation', tabParams: { editing: false } });
                    }}
                />
            )}
        </>
    );
};
