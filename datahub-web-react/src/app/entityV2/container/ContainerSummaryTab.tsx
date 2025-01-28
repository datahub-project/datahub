import React from 'react';
import { useEntityData } from '../../entity/shared/EntityContext';
import { SubType } from '../shared/components/subtypes';
import TableauWorkbookSummaryTab from './tableau/TableauWorkbookSummaryTab';

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
