import React from 'react';
import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { getAssetDescriptionDetails } from '@src/app/entityV2/shared/tabs/Documentation/utils';
import { useShouldShowInferDocumentationButton } from '@src/app/entityV2/shared/components/inferredDocs/utils';
import InferenceDetailsPill from '@src/app/sharedV2/inferred/InferenceDetailsPill';
import styled from 'styled-components';
import { Button } from 'antd';
import { Sparkle } from 'phosphor-react';
import { useEntityData, useRouteToTab } from '../../../../../../entity/shared/EntityContext';
import DescriptionSection from './DescriptionSection';
import LinksSection from './LinksSection';
import SourceRefSection from './SourceRefSection';
import { SidebarSection } from '../SidebarSection';
import { EMPTY_MESSAGES } from '../../../../constants';
import SectionActionButton from '../SectionActionButton';
import EmptySectionText from '../EmptySectionText';

const LINE_LIMIT = 5;

const InferDescriptionButton = styled(Button)`
    display: flex;
    padding-left: 8px;
    padding-right: 8px;
    background-color: #f1f3fd;
    border-radius: 8px;
    align-items: center;
    justify-content: flex-start;
    box-shadow: none;
    margin-top: 8px;
    border: 0;
    &:hover,
    &:focus {
        box-shadow: none;
        border: 0;
        background-color: #f9f3fd;
    }
    span,
    path {
        color: #5c3fd1 !important;
    }
`;

const AiSparkle = styled(Sparkle)`
    height: 14px;
    width: 14px;
    margin-right: 4px;
`;

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
                                <InferDescriptionButton
                                    onClick={() =>
                                        routeToTab({
                                            tabName: 'Documentation',
                                            tabParams: { editing: true, inferOnMount: true },
                                        })
                                    }
                                >
                                    <AiSparkle />
                                    Generate with AI
                                </InferDescriptionButton>
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
