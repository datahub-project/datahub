import { Button } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import React from 'react';
import { useEntityData, useRouteToTab } from '../../../../EntityContext';
import { SidebarHeader } from '../SidebarHeader';
import DescriptionSection from './DescriptionSection';
import LinksSection from './LinksSection';
import SourceRefSection from './SourceRefSection';
import EmptyContentSection from './EmptyContentSection';

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
