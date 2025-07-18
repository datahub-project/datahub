import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { Entity } from '@types';

export function useGetEntities(urns: string[]): {
    entities: Entity[];
    loading: boolean;
} {
    const verifiedUrns = urns.filter((urn) => typeof urn === 'string' && urn.startsWith('urn:li:'));

    const { data, loading } = useGetEntitiesQuery({ variables: { urns: verifiedUrns }, skip: !verifiedUrns.length });
    const entities: Entity[] = Array.isArray(data?.entities) ? (data?.entities.filter(Boolean) as Entity[]) : [];
    return { entities, loading };
}
