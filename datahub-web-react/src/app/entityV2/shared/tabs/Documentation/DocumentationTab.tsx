import { EditOutlined, ExpandAltOutlined, PlusOutlined } from '@ant-design/icons';
import { Button as AntButton, Typography } from 'antd';
import queryString from 'query-string';
import React, { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityData, useRouteToTab } from '@app/entity/shared/EntityContext';
import { EmptyTab } from '@app/entityV2/shared/components/styled/EmptyTab';
import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import { DescriptionEditor } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionEditor';
import { DescriptionPreviewModal } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionPreviewModal';
import { RelatedSection } from '@app/entityV2/shared/tabs/Documentation/components/RelatedSection';
import { getAssetDescriptionDetails } from '@app/entityV2/shared/tabs/Documentation/utils';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '@app/entityV2/shared/utils';
import { Button, Editor } from '@src/alchemy-components';

const DOCUMENTATION_TAB_NAME = 'Documentation';
const DOCUMENTATION_TAB = 'documentation';

const DocumentationContainer = styled.div`
    margin: 0 16px;
    padding: 32px 0;
    max-width: calc(100% - 10px);
`;

const StyledTabToolbar = styled(TabToolbar)`
    background-color: ${(props) => props.theme.colors.bgSurface};
    border-top-left-radius: 4px;
    border-bottom-left-radius: 4px;
    border-left: 2px solid ${(props) => props.theme.colors.borderBrand};
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
    const { t } = useTranslation('entity.profile.documentation');
    const { t: tc } = useTranslation('common.actions');
    const hideLinksButton = properties?.hideLinksButton;
    const { urn, entityData } = useEntityData();

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
                tabName: DOCUMENTATION_TAB_NAME,
                tabParams: { editing: true, modal: !!showModal },
            });
        }
    }, [urn, routeToTab, showModal, localStorageDictionary]);

    return isEditing && !showModal ? (
        <DescriptionEditor onComplete={() => routeToTab({ tabName: DOCUMENTATION_TAB_NAME })} />
    ) : (
        <>
            {displayedDescription || links.length ? (
                <>
                    <StyledTabToolbar>
                        <div>
                            <AntButton
                                data-testid="edit-documentation-button"
                                type="text"
                                onClick={() =>
                                    routeToTab({ tabName: DOCUMENTATION_TAB_NAME, tabParams: { editing: true } })
                                }
                            >
                                <EditOutlined /> {tc('edit')}
                            </AntButton>
                        </div>
                        <div>
                            <AntButton
                                type="text"
                                onClick={() =>
                                    routeToTab({
                                        tabName: DOCUMENTATION_TAB_NAME,
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
                                <Editor
                                    content={displayedDescription}
                                    dataTestId="documentation-editor-content"
                                    readOnly
                                />,
                            ]
                        ) : (
                            <DocumentationContainer>
                                <Typography.Text type="secondary">{t('emptyState')}</Typography.Text>
                            </DocumentationContainer>
                        )}
                        {!hideLinksButton && <RelatedSection />}
                    </div>
                </>
            ) : (
                <EmptyTabWrapper>
                    <EmptyTab tab={DOCUMENTATION_TAB} hideImage={false}>
                        <Button
                            data-testid="add-documentation"
                            onClick={() =>
                                routeToTab({ tabName: DOCUMENTATION_TAB_NAME, tabParams: { editing: true } })
                            }
                        >
                            <PlusOutlined /> {t('addDocumentation')}
                        </Button>
                    </EmptyTab>
                </EmptyTabWrapper>
            )}
            {showModal && (
                <DescriptionPreviewModal
                    editMode={(isEditing && true) || false}
                    description={displayedDescription}
                    onClose={() => {
                        routeToTab({ tabName: DOCUMENTATION_TAB_NAME, tabParams: { editing: false } });
                    }}
                />
            )}
        </>
    );
};
