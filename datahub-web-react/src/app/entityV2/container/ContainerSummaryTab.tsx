/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import TableauWorkbookSummaryTab from '@app/entityV2/container/tableau/TableauWorkbookSummaryTab';
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
