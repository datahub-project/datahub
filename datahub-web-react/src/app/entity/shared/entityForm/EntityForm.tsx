/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { FormView, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import FormByEntity from '@app/entity/shared/entityForm/FormByEntity';

interface Props {
    formUrn: string;
}

export default function EntityForm({ formUrn }: Props) {
    const { formView } = useEntityFormContext();

    if (formView === FormView.BY_ENTITY) return <FormByEntity formUrn={formUrn} />;

    return null;
}
