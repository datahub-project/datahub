import React from 'react';

import ChildrenTab from '@app/entityV2/glossaryNode/ChildrenTab';
import { useEntityData } from '@app/entityV2/shared/EntityContext';

export default function ChildrenTabWrapper() {
    const { urn } = useEntityData();

    return <ChildrenTab key={urn} />;
}
