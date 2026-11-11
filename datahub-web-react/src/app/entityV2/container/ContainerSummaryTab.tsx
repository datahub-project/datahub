import React from 'react';

import TableauWorkbookSummaryTab from '@app/entityV2/container/tableau/TableauWorkbookSummaryTab';
import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { SubType } from '@app/entityV2/shared/components/subtypes';
import { getFirstSubType } from '@app/entityV2/shared/utils';

export default function ContainerSummaryTab() {
    const { entityData } = useEntityData();
    const subtype = getFirstSubType(entityData);
    switch (subtype) {
        case SubType.TableauWorkbook:
            return <TableauWorkbookSummaryTab />;
        default:
            return null;
    }
}
