import React from 'react';
import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { getAssetDescriptionDetails } from '@src/app/entityV2/shared/tabs/Documentation/utils';
import { useShouldShowInferDocumentationButton } from '@src/app/entityV2/shared/components/inferredDocs/utils';
import InferenceDetailsPill from '@src/app/sharedV2/inferred/InferenceDetailsPill';
import InferDocsButton from '@src/app/entityV2/shared/components/inferredDocs/InferDocsButton';
import { useEntityData, useRouteToTab } from '../../../../../../entity/shared/EntityContext';
import DescriptionSection from './DescriptionSection';
import LinksSection from './LinksSection';
import SourceRefSection from './SourceRefSection';
import { SidebarSection } from '../SidebarSection';
import { EMPTY_MESSAGES } from '../../../../constants';
import SectionActionButton from '../SectionActionButton';
import EmptySectionText from '../EmptySectionText';

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
    const hideLinksButton = properties?.hideLinksButton;

    const canShowInferDocsButton = useShouldShowInferDocumentationButton(entityType);

    const { displayedDescription, isInferred } = getAssetDescriptionDetails({
        entityProperties: entityData,
        enableInferredDescriptions: canShowInferDocsButton,
    });
    const showInferDocsButton = !displayedDescription && canShowInferDocsButton;

    const links = entityData?.institutionalMemory?.elements || [];

    const hasContent = !!displayedDescription || links.length > 0;
    const routeToTab = useRouteToTab();

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
                        {hasContent && <LinksSection hideLinksButton={hideLinksButton} readOnly />}
                        {!hasContent && [
                            <EmptySectionText message={EMPTY_MESSAGES.documentation.title} />,
                            showInferDocsButton && (
                                <InferDocsButton
                                    style={{ height: 32, width: 128, marginTop: 8 }}
                                    onClick={() =>
                                        routeToTab({
                                            tabName: 'Documentation',
                                            tabParams: { editing: true, inferOnMount: true },
                                        })
                                    }
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
                                onClick={(event) => {
                                    routeToTab({ tabName: 'Documentation', tabParams: { editing: true } });
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
