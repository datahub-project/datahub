import React from 'react';
import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { getAssetDescriptionDetails } from '@src/app/entityV2/shared/tabs/Documentation/utils';
import { useIsEmbeddedProfile } from '@src/app/shared/useEmbeddedProfileLinkProps';
import useIsLineageMode from '@src/app/lineage/utils/useIsLineageMode';
import { useIsSeparateSiblingsMode } from '@src/app/entity/shared/siblingUtils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import DescriptionSection from './DescriptionSection';
import LinksSection from './LinksSection';
import SourceRefSection from './SourceRefSection';
import { SidebarSection } from '../SidebarSection';
import { EMPTY_MESSAGES } from '../../../../constants';
import SectionActionButton from '../SectionActionButton';
import EmptySectionText from '../EmptySectionText';
import { useEntityData, useMutationUrn, useRouteToTab } from '../../../../../../entity/shared/EntityContext';
import { getEntityPath } from '../../utils';

const LINE_LIMIT = 5;

interface Properties {
    hideLinksButton?: boolean;
}

interface Props {
    properties?: Properties;
    readOnly?: boolean;
}

export const SidebarAboutSection = ({ properties, readOnly }: Props) => {
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

    const links = entityData?.institutionalMemory?.elements || [];

    const hasContent = !!displayedDescription || links.length > 0;

    const canEditDescription = !!entityData?.privileges?.canEditDescription;

    return (
        <>
            <SidebarSection
                title="Documentation"
                content={
                    <>
                        {displayedDescription && [
                            <DescriptionSection
                                description={displayedDescription}
                                isExpandable
                                lineLimit={LINE_LIMIT}
                            />,
                        ]}
                        {hasContent && <LinksSection hideLinksButton={hideLinksButton} readOnly />}
                        {!hasContent && [<EmptySectionText message={EMPTY_MESSAGES.documentation.title} />]}
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

export default SidebarAboutSection;
