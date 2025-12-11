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
import Tag from '@app/sharedV2/tags/tag/Tag';

import { TagAssociation } from '@types';

export default function TagsProperty(props: PropertyComponentProps) {
    const { entityData, loading } = useEntityContext();
    const tagAssociations = entityData?.globalTags?.tags ?? [];

    const renderTag = (tagAssociation: TagAssociation) => {
        return <Tag tag={tagAssociation} options={{ shouldNotAddBottomMargin: true }} />;
    };

    return (
        <BaseProperty
            {...props}
            values={tagAssociations}
            renderValue={renderTag}
            loading={loading}
            restItemsPillBorderType="rounded"
        />
    );
}
