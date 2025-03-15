import { decodeUrn } from '@app/entity/shared/utils';
import EntityPageWrapper from '@app/EntityPageWrapper';
import { EntityType } from '@types';
import React from 'react';
import { useParams } from 'react-router';

interface RouteParams {
    urn: string;
}

interface Props {
    entityType: EntityType;
}

export default function TypedEntityPage({ entityType }: Props) {
    const { urn: encodedUrn } = useParams<RouteParams>();
    return <EntityPageWrapper urn={decodeUrn(encodedUrn)} entityType={entityType} />;
}
