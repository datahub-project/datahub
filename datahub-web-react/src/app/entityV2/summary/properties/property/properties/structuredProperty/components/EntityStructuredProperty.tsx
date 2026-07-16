import React, { useMemo } from 'react';

import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { StructuredPropertyComponentProps } from '@app/entityV2/summary/properties/property/properties/structuredProperty/types';
import { CompactEntityNameComponent } from '@app/recommendations/renderer/component/CompactEntityNameComponent';
import { useGetEntities } from '@app/sharedV2/useGetEntities';

import { Entity } from '@types';

export default function EntityStructuredProperty({
    structuredPropertyEntry,
    ...props
}: StructuredPropertyComponentProps) {
    const urns = useMemo(
        () =>
            (structuredPropertyEntry?.valueEntities?.map((entity) => entity?.urn) ?? []).filter(
                (urn): urn is string => !!urn,
            ),
        [structuredPropertyEntry?.valueEntities],
    );

    const renderEntity = (entity: Entity) => <CompactEntityNameComponent entity={entity} showMargin={false} />;

    const { entities } = useGetEntities(urns);

    return <BaseProperty {...props} values={entities} renderValue={renderEntity} restItemsPillBorderType="rounded" />;
}
