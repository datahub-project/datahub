import { EditOutlined, ExpandAltOutlined } from '@ant-design/icons';
import { Button, Divider, Typography } from 'antd';
import queryString from 'query-string';
import React, { useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityData, useRefetch, useRouteToTab } from '@app/entity/shared/EntityContext';
import { AddLinkModal } from '@app/entity/shared/components/styled/AddLinkModal';
import { EmptyTab } from '@app/entity/shared/components/styled/EmptyTab';
import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import { DescriptionEditor } from '@app/entity/shared/tabs/Documentation/components/DescriptionEditor';
import { DescriptionPreviewModal } from '@app/entity/shared/tabs/Documentation/components/DescriptionPreviewModal';
import { LinkList } from '@app/entity/shared/tabs/Documentation/components/LinkList';
import { Editor } from '@app/entity/shared/tabs/Documentation/components/editor/Editor';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '@app/entity/shared/utils';

const DocumentationContainer = styled.div`
    margin: 0 32px;
    padding: 40px 0;
    max-width: calc(100% - 10px);
`;

interface Props {
    hideLinksButton?: boolean;
}

export const DocumentationTab = ({ properties }: { properties?: Props }) => {
    const hideLinksButton = properties?.hideLinksButton;
    const { urn, entityData } = useEntityData();
    const refetch = useRefetch();
    const description = entityData?.editableProperties?.description || entityData?.properties?.description || '';
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
        <>
            <DescriptionEditor onComplete={() => routeToTab({ tabName: 'Documentation' })} />
        </>
    ) : (
        <>
            {description || links.length ? (
                <>
                    <TabToolbar>
                        <div>
                            <Button
                                data-testid="edit-documentation-button"
                                type="text"
                                onClick={() => routeToTab({ tabName: 'Documentation', tabParams: { editing: true } })}
                            >
                                <EditOutlined /> Edit
                            </Button>
                            {!hideLinksButton && <AddLinkModal buttonProps={{ type: 'text' }} refetch={refetch} />}
                        </div>
                        <div>
                            <Button
                                type="text"
                                onClick={() =>
                                    routeToTab({
                                        tabName: 'Documentation',
                                        tabParams: { modal: true },
                                    })
                                }
                            >
                                <ExpandAltOutlined />
                            </Button>
                        </div>
                    </TabToolbar>
                    <div>
                        {description ? (
                            <Editor content={description} readOnly />
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
                <EmptyTab tab="documentation">
                    <Button onClick={() => routeToTab({ tabName: 'Documentation', tabParams: { editing: true } })}>
                        <EditOutlined /> Add Documentation
                    </Button>
                    {!hideLinksButton && <AddLinkModal refetch={refetch} />}
                </EmptyTab>
            )}
            {showModal && (
                <DescriptionPreviewModal
                    editMode={(isEditing && true) || false}
                    description={description}
                    onClose={() => {
                        routeToTab({ tabName: 'Documentation', tabParams: { editing: false } });
                    }}
                />
            )}
        </>
    );
};
