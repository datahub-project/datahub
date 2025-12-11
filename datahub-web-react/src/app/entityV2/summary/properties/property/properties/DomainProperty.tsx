/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';

import { Domain } from '@types';

export default function DomainProperty(props: PropertyComponentProps) {
    const { entityData, loading } = useEntityContext();
    const domain = entityData?.domain?.domain;

    const renderTag = (tagAssociation: Domain) => {
        return <DomainLink domain={tagAssociation} />;
    };

    return <BaseProperty {...props} values={domain ? [domain] : []} renderValue={renderTag} loading={loading} />;
}
