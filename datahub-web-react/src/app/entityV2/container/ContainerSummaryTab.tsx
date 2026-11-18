import React from 'react';

import TableauWorkbookSummaryTab from '@app/entityV2/container/tableau/TableauWorkbookSummaryTab';
import WorkbookSummaryTab from '@app/entityV2/container/workbook/WorkbookSummaryTab';
import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { SubType } from '@app/entityV2/shared/components/subtypes';
import { getFirstSubType } from '@app/entityV2/shared/utils';

export default function ContainerSummaryTab() {
    const { entityData } = useEntityData();
    const subtype = getFirstSubType(entityData);
    if (subtype === SubType.TableauWorkbook) {
        return <TableauWorkbookSummaryTab />;
    }
    // A workbook from another platform, such as a Qlik app, whose own subtype
    // comes first: the Tableau sections would be empty for it.
    if (entityData?.subTypes?.typeNames?.includes(SubType.TableauWorkbook)) {
        return <WorkbookSummaryTab />;
    }
    return null;
}
