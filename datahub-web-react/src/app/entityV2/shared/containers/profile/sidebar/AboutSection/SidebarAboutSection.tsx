import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { useEntityData, useMutationUrn, useRouteToTab } from '@app/entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import { getEntityPath } from '@app/entityV2/shared/containers/profile/entityData';
import DescriptionSection from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/DescriptionSection';
import LinksSection from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/LinksSection';
import SourceRefSection from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SourceRefSection';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { useDocumentationPermission } from '@app/entityV2/summary/documentation/useDocumentationPermission';
import { useEntityHasSummaryTab } from '@app/entityV2/summary/useEntityHasSummaryTab';
import { useIsSeparateSiblingsMode } from '@src/app/entity/shared/siblingUtils';
import { getAssetDescriptionDetails } from '@src/app/entityV2/shared/tabs/Documentation/utils';
import useIsLineageMode from '@src/app/lineage/utils/useIsLineageMode';
import { useIsEmbeddedProfile } from '@src/app/shared/useEmbeddedProfileLinkProps';
import { useEntityRegistry } from '@src/app/useEntityRegistry';

const LINE_LIMIT = 5;

/* eslint-disable i18next/no-literal-string -- route tab name identifiers, not UI text */
const SUMMARY_TAB = 'Summary';
const DOCUMENTATION_TAB = 'Documentation';
/* eslint-enable i18next/no-literal-string */

interface Properties {
    hideLinksButton?: boolean;
}

interface Props {
    properties?: Properties;
    readOnly?: boolean;
}

export const SidebarAboutSection = ({ properties, readOnly }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const { entityData, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const isLineageMode = useIsLineageMode();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const urn = useMutationUrn();

    const hideLinksButton = properties?.hideLinksButton;
    const isEmbeddedProfile = useIsEmbeddedProfile();
    const routeToTab = useRouteToTab();

    const { displayedDescription } = getAssetDescriptionDetails({
        entityProperties: entityData,
    });

    const hasContent = useMemo(() => {
        // Do not take into account links that shown in entity profile's header as they will not be shown
        const links =
            entityData?.institutionalMemory?.elements?.filter((link) => !link.settings?.showInAssetPreview) || [];

        return !!displayedDescription || links.length > 0;
    }, [displayedDescription, entityData]);

    const canEditDescription = useDocumentationPermission();

    // Edit where this profile actually renders documentation: the Summary tab when it has one,
    // otherwise the Documentation tab.
    const hasSummaryTab = useEntityHasSummaryTab(entityType);
    const editTab = hasSummaryTab ? SUMMARY_TAB : DOCUMENTATION_TAB;
    const editTabParams = hasSummaryTab ? { editingDescription: true } : { editing: true };

    return (
        <>
            <SidebarSection
                title={t('sidebar.about.documentationTitle')}
                content={
                    <>
                        {displayedDescription && (
                            <DescriptionSection
                                description={displayedDescription}
                                isExpandable
                                lineLimit={LINE_LIMIT}
                            />
                        )}
                        {hasContent && <LinksSection hideLinksButton={hideLinksButton} readOnly />}
                        {!hasContent && <EmptySectionText message={EMPTY_MESSAGES.documentation.title} />}
                    </>
                }
                extra={
                    <>
                        {!readOnly && (
                            <SectionActionButton
                                icon={hasContent ? PencilSimple : Plus}
                                dataTestId="editDocumentation"
                                onClick={(event) => {
                                    if (!isEmbeddedProfile) {
                                        routeToTab({ tabName: editTab, tabParams: editTabParams });
                                    } else {
                                        const url = getEntityPath(
                                            entityType,
                                            urn,
                                            entityRegistry,
                                            isLineageMode,
                                            isHideSiblingMode,
                                            editTab,
                                            editTabParams,
                                        );
                                        window.open(url, '_blank');
                                    }
                                    event.stopPropagation();
                                }}
                                actionPrivilege={canEditDescription}
                            />
                        )}
                    </>
                }
            />
            <SourceRefSection />
        </>
    );
};
