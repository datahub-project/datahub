import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import ChildrenTab from '@app/entityV2/glossaryNode/ChildrenTab';

export default function ChildrenTabWrapper() {
    const { urn } = useEntityData();

    return <ChildrenTab key={urn} />;
}
