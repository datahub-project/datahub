import React, { useContext } from 'react';
import { SidebarSection } from '../SidebarSection';
import EntitySidebarContext from '../../../../../../sharedV2/EntitySidebarContext';

export default function SidebarOperationSection() {
    const { extra } = useContext(EntitySidebarContext);
    console.log(extra);

    if (!extra?.transformOperation) {
        return null;
    }

    return <SidebarSection title="Operation" content={<>{extra.transformOperation}</>} />;
}
