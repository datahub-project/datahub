import { decodeUrn } from '@app/entity/shared/utils';
import EntityPageWrapper from '@app/EntityPageWrapper';
import { NoPageFound } from '@app/shared/NoPageFound';
import { useEntityRegistry } from '@app/useEntityRegistry';
import React, { useMemo } from 'react';
import { useParams } from 'react-router';

interface GenericEntityPageRouteParams {
    urnWithoutPrefix: string;
}

export const GENERIC_ENTITY_PAGE_PATH = '/urn\\::urnWithoutPrefix/:tab?';

export default function GenericEntityPage() {
    const entityRegistry = useEntityRegistry();
    const { urnWithoutPrefix } = useParams<GenericEntityPageRouteParams>();
    const encodedUrn = `urn:${urnWithoutPrefix}`;

    const urn = useMemo(() => decodeUrn(encodedUrn), [encodedUrn]);
    const entityType = useMemo(() => {
        const parts = urn.split(':', 3);
        const [, , type] = parts;
        return entityRegistry.getTypeFromGraphName(type);
    }, [entityRegistry, urn]);

    if (!entityType) {
        console.debug(`Failed to decode urn: ${encodedUrn}`);
        return <NoPageFound />;
    }

    return <EntityPageWrapper urn={urn} entityType={entityType} />;
}
