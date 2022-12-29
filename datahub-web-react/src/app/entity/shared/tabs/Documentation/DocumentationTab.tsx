import React, { useEffect } from 'react';
import queryString from 'query-string';
import { useLocation } from 'react-router-dom';

import styled from 'styled-components';
import { Button, Divider, Typography } from 'antd';
import { EditOutlined } from '@ant-design/icons';

import TabToolbar from '../../components/styled/TabToolbar';
import { AddLinkModal } from '../../components/styled/AddLinkModal';
import { EmptyTab } from '../../components/styled/EmptyTab';
import { DescriptionEditor } from './components/DescriptionEditor';
import { LinkList } from './components/LinkList';

import { useEntityData, useRefetch, useRouteToTab } from '../../EntityContext';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '../../utils';
import { Editor } from './components/editor/Editor';

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

    useEffect(() => {
        const editedDescriptions = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};
        if (editedDescriptions.hasOwnProperty(urn)) {
            routeToTab({ tabName: 'Documentation', tabParams: { editing: true } });
        }
    }, [urn, routeToTab, localStorageDictionary]);

    return isEditing ? (
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
                                type="text"
                                onClick={() => routeToTab({ tabName: 'Documentation', tabParams: { editing: true } })}
                            >
                                <EditOutlined /> Edit
                            </Button>
                            {!hideLinksButton && <AddLinkModal buttonProps={{ type: 'text' }} refetch={refetch} />}
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
        </>
    );
};
