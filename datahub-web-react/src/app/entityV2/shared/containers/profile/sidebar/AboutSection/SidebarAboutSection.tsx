import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import React, { useMemo } from 'react';

import { useEntityData, useMutationUrn, useRouteToTab } from '@app/entity/shared/EntityContext';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import DescriptionSection from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/DescriptionSection';
import LinksSection from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/LinksSection';
import SourceRefSection from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/SourceRefSection';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { getEntityPath } from '@app/entityV2/shared/containers/profile/utils';
import { useIsSeparateSiblingsMode } from '@src/app/entity/shared/siblingUtils';
import InferDocsButton from '@src/app/entityV2/shared/components/inferredDocs/InferDocsButton';
import { useShouldShowInferDocumentationButton } from '@src/app/entityV2/shared/components/inferredDocs/utils';
import { getAssetDescriptionDetails } from '@src/app/entityV2/shared/tabs/Documentation/utils';
import useIsLineageMode from '@src/app/lineage/utils/useIsLineageMode';
import { useIsEmbeddedProfile } from '@src/app/shared/useEmbeddedProfileLinkProps';
import InferenceDetailsPill from '@src/app/sharedV2/inferred/InferenceDetailsPill';
import { useEntityRegistry } from '@src/app/useEntityRegistry';

const LINE_LIMIT = 5;

interface Props {
    readOnly?: boolean;
}

export const SidebarAboutSection = ({ readOnly: readOnlyFromProps }: Props) => {
    const { isInFormContext } = useEntityFormContext();
    const readOnly = readOnlyFromProps || isInFormContext;
    const { entityData, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const isLineageMode = useIsLineageMode();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const urn = useMutationUrn();

    const isEmbeddedProfile = useIsEmbeddedProfile();
    const routeToTab = useRouteToTab();

    const canShowInferDocsButton = useShouldShowInferDocumentationButton(entityType);

    const { displayedDescription, isInferred } = getAssetDescriptionDetails({
        entityProperties: entityData,
        enableInferredDescriptions: canShowInferDocsButton,
    });
    const showInferDocsButton = !displayedDescription && canShowInferDocsButton;

    const hasContent = useMemo(() => {
        // Do not take into account links that shown in entity profile's header as they will not be shown
        const links =
            entityData?.institutionalMemory?.elements?.filter((link) => !link.settings?.showInAssetPreview) || [];

        return !!displayedDescription || links.length > 0;
    }, [displayedDescription, entityData]);

    const canEditDescription = !!entityData?.privileges?.canEditDescription;
    const canProposeDescription = !!entityData?.privileges?.canProposeDescription;

    return (
        <>
            <SidebarSection
                title="Documentation"
                content={
                    <>
                        {displayedDescription && [
                            isInferred && <InferenceDetailsPill pillStyles={{ marginBottom: 4 }} />,
                            <DescriptionSection
                                description={displayedDescription}
                                isExpandable
                                lineLimit={LINE_LIMIT}
                            />,
                        ]}
                        {hasContent && <LinksSection readOnly />}
                        {!hasContent && [
                            <EmptySectionText message={EMPTY_MESSAGES.documentation.title} />,
                            showInferDocsButton && (
                                <InferDocsButton
                                    style={{ height: 32, width: 128, marginTop: 8 }}
                                    surface="entity-sidebar"
                                    onClick={() => {
                                        if (!isEmbeddedProfile) {
                                            routeToTab({
                                                tabName: 'Documentation',
                                                tabParams: { editing: true, inferOnMount: true },
                                            });
                                        } else {
                                            const url = getEntityPath(
                                                entityType,
                                                urn,
                                                entityRegistry,
                                                isLineageMode,
                                                isHideSiblingMode,
                                                'Documentation',
                                                {
                                                    editing: true,
                                                    inferOnMount: true,
                                                },
                                            );
                                            window.open(url, '_blank');
                                        }
                                    }}
                                />
                            ),
                        ]}
                    </>
                }
                extra={
                    <>
                        {!readOnly && (
                            <SectionActionButton
                                button={hasContent ? <EditOutlinedIcon /> : <AddRoundedIcon />}
                                dataTestId="editDocumentation"
                                onClick={(event) => {
                                    if (!isEmbeddedProfile) {
                                        routeToTab({ tabName: 'Documentation', tabParams: { editing: true } });
                                    } else {
                                        const url = getEntityPath(
                                            entityType,
                                            urn,
                                            entityRegistry,
                                            isLineageMode,
                                            isHideSiblingMode,
                                            'Documentation',
                                            {
                                                editing: true,
                                            },
                                        );
                                        window.open(url, '_blank');
                                    }
                                    event.stopPropagation();
                                }}
                                actionPrivilege={canEditDescription || canProposeDescription}
                            />
                        )}
                    </>
                }
            />
            <SourceRefSection />
        </>
    );
};

export default SidebarAboutSection;
