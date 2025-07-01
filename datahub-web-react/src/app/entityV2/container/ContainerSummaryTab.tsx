import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import TableauWorkbookSummaryTab from '@app/entityV2/container/tableau/TableauWorkbookSummaryTab';
import { SubType } from '@app/entityV2/shared/components/subtypes';

export default function ContainerSummaryTab() {
    const { entityData } = useEntityData();
    const subtype = entityData?.subTypes?.typeNames?.[0];
    switch (subtype) {
        case SubType.TableauWorkbook:
            return <TableauWorkbookSummaryTab />;
        default:
            return null;
    }
}
