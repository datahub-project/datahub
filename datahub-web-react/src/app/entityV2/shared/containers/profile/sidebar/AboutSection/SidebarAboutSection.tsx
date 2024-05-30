import React from 'react';
import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
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
    const { entityData } = useEntityData();
    const hideLinksButton = properties?.hideLinksButton;
    const originalDescription = entityData?.properties?.description;
    const editedDescription = entityData?.editableProperties?.description;
    const description = editedDescription || originalDescription || '';
    const links = entityData?.institutionalMemory?.elements || [];

    const hasContent = !!description || links.length > 0;
    const routeToTab = useRouteToTab();

    const canEditDescription = !!entityData?.privileges?.canEditDescription;

    return (
        <>
            <SidebarSection
                title="Documentation"
                content={
                    <>
                        {description && (
                            <DescriptionSection description={description} isExpandable lineLimit={LINE_LIMIT} />
                        )}
                        {hasContent && <LinksSection hideLinksButton={hideLinksButton} readOnly />}
                        {!hasContent && <EmptySectionText message={EMPTY_MESSAGES.documentation.title} />}
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
