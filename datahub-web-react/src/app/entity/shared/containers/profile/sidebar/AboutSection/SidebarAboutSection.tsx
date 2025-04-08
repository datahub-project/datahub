import { EditOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';

import { useEntityData, useRouteToTab } from '@app/entity/shared/EntityContext';
import DescriptionSection from '@app/entity/shared/containers/profile/sidebar/AboutSection/DescriptionSection';
import EmptyContentSection from '@app/entity/shared/containers/profile/sidebar/AboutSection/EmptyContentSection';
import LinksSection from '@app/entity/shared/containers/profile/sidebar/AboutSection/LinksSection';
import SourceRefSection from '@app/entity/shared/containers/profile/sidebar/AboutSection/SourceRefSection';
import { SidebarHeader } from '@app/entity/shared/containers/profile/sidebar/SidebarHeader';

interface Properties {
    hideLinksButton?: boolean;
}

interface Props {
    properties?: Properties;
    readOnly?: boolean;
}

export const SidebarAboutSection = ({ properties, readOnly }: Props) => {
    const hideLinksButton = properties?.hideLinksButton;
    const { entityData } = useEntityData();
    const routeToTab = useRouteToTab();

    const originalDescription = entityData?.properties?.description;
    const editedDescription = entityData?.editableProperties?.description;
    const description = editedDescription || originalDescription || '';
    const links = entityData?.institutionalMemory?.elements || [];

    const hasContent = !!description || links.length > 0;

    return (
        <div>
            <SidebarHeader
                title="About"
                actions={
                    hasContent &&
                    !readOnly && (
                        <Button
                            onClick={() => routeToTab({ tabName: 'Documentation', tabParams: { editing: true } })}
                            type="text"
                            shape="circle"
                        >
                            <EditOutlined />
                        </Button>
                    )
                }
            />
            {description && <DescriptionSection description={description} />}
            {!hasContent && <EmptyContentSection hideLinksButton={hideLinksButton} readOnly={readOnly} />}
            {hasContent && <LinksSection hideLinksButton={hideLinksButton} readOnly={readOnly} />}
            <SourceRefSection />
        </div>
    );
};

export default SidebarAboutSection;
