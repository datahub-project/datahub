import React from 'react';
import { useEntityData } from '../../../../EntityContext';
import DescriptionSection from './DescriptionSection';
import LinksSection from './LinksSection';
import SourceRefSection from './SourceRefSection';
import EmptyContentSection from './EmptyContentSection';
import { SidebarSection } from '../SidebarSection';

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

    return (
        <>
            <SidebarSection
                title="About"
                content={
                    <>
                        {description && <DescriptionSection description={description} />}
                        {hasContent && <LinksSection hideLinksButton={hideLinksButton} readOnly />}
                        {!hasContent && <EmptyContentSection readOnly={readOnly} />}
                    </>
                }
            />
            <SourceRefSection />
        </>
    );
};

export default SidebarAboutSection;
