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
