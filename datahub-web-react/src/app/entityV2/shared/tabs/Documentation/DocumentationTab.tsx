import queryString from 'query-string';
import React, { useEffect } from 'react';
import { Button } from '@src/alchemy-components';
import { useLocation } from 'react-router-dom';

import { EditOutlined, ExpandAltOutlined, PlusOutlined } from '@ant-design/icons';
import { Button as AntButton, Divider, Typography } from 'antd';
import styled from 'styled-components';
import InferenceDetailsPill from '@src/app/sharedV2/inferred/InferenceDetailsPill';

import { AddLinkModal } from '../../components/styled/AddLinkModal';
import { EmptyTab } from '../../components/styled/EmptyTab';
import TabToolbar from '../../components/styled/TabToolbar';
import { DescriptionEditor } from './components/DescriptionEditor';
import { LinkList } from './components/LinkList';

import { useEntityData, useRefetch, useRouteToTab } from '../../../../entity/shared/EntityContext';
import { REDESIGN_COLORS } from '../../constants';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '../../utils';
import { DescriptionPreviewModal } from './components/DescriptionPreviewModal';
import { Editor } from './components/editor/Editor';
import {
    useIsDocumentationInferenceEnabled,
    useShouldShowInferDocumentationButton,
} from '../../components/inferredDocs/utils';
import { getAssetDescriptionDetails } from './utils';
import InferDocsButton from '../../components/inferredDocs/InferDocsButton';

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
    const { urn, entityData, entityType } = useEntityData();

    const refetch = useRefetch();
    const { displayedDescription, isInferred } = getAssetDescriptionDetails({
        entityProperties: entityData,
        enableInferredDescriptions: useIsDocumentationInferenceEnabled(),
    });
    const links = entityData?.institutionalMemory?.elements || [];
    const localStorageDictionary = localStorage.getItem(EDITED_DESCRIPTIONS_CACHE_NAME);

    const routeToTab = useRouteToTab();
    const isEditing = queryString.parse(useLocation().search, { parseBooleans: true }).editing;
    const showModal = queryString.parse(useLocation().search, { parseBooleans: true }).modal;
    const { inferOnMount } = queryString.parse(useLocation().search, { parseBooleans: true });

    const shouldShowInferenceButton = useShouldShowInferDocumentationButton(entityType);

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
        <DescriptionEditor inferOnMount={!!inferOnMount} onComplete={() => routeToTab({ tabName: 'Documentation' })} />
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
                            [
                                isInferred && <InferenceDetailsPill pillStyles={{ marginTop: 16, marginLeft: 24 }} />,
                                <Editor content={displayedDescription} readOnly />,
                            ]
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
                        {shouldShowInferenceButton && (
                            <InferDocsButton
                                style={{ height: 38 }}
                                surface="entity-docs-tab"
                                onClick={() =>
                                    routeToTab({
                                        tabName: 'Documentation',
                                        tabParams: { editing: true, inferOnMount: true },
                                    })
                                }
                            />
                        )}
                    </EmptyTab>
                </EmptyTabWrapper>
            )}
            {showModal && (
                <DescriptionPreviewModal
                    editMode={(isEditing && true) || false}
                    description={displayedDescription}
                    isInferred={isInferred}
                    onClose={() => {
                        routeToTab({ tabName: 'Documentation', tabParams: { editing: false } });
                    }}
                />
            )}
        </>
    );
};
