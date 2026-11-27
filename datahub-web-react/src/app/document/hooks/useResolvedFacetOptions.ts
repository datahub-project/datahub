import { useMemo, useRef } from 'react';

import { FacetSelectOption } from '@app/document/utils/documentSidebarFacets.utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useGetEntities } from '@src/app/sharedV2/useGetEntities';

/**
 * Names for selected facet values that the current facet aggregations did not return, so a sidebar filter keeps
 * showing a name (not a urn) after a search or another filter drops the value from the facet.
 *
 * Options seen in earlier facet responses are remembered and reused, so the common case (a value that was in the
 * facet and dropped out) needs no request. Only urns never seen by this sidebar (e.g. a filter restored from the URL
 * whose value is not in the first facet response) are looked up. Non-urn values (e.g. sub-type names) are ignored.
 */
export function useResolvedFacetOptions(
    urns: string[],
    facetOptions: Record<string, FacetSelectOption[]>,
): Map<string, FacetSelectOption> {
    const entityRegistry = useEntityRegistry();
    const seenOptionsRef = useRef(new Map<string, FacetSelectOption>());

    const rememberedOptions = useMemo(() => {
        const seen = seenOptionsRef.current;
        Object.values(facetOptions).forEach((options) =>
            options.forEach((option) => {
                // Only options backed by an entity carry a real name; others are labelled by their raw value.
                if (option.entity) seen.set(option.value, option);
            }),
        );
        return new Map(
            urns.flatMap((urn) => {
                const option = seen.get(urn);
                return option ? [[urn, option] as const] : [];
            }),
        );
    }, [urns, facetOptions]);

    const unseenUrns = useMemo(() => urns.filter((urn) => !rememberedOptions.has(urn)), [urns, rememberedOptions]);
    const { entities } = useGetEntities(unseenUrns, undefined, { skipLineage: true, skipSiblingsSearch: true });

    return useMemo(
        () =>
            new Map([
                ...rememberedOptions,
                ...entities.map(
                    (entity) =>
                        [
                            entity.urn,
                            { value: entity.urn, label: entityRegistry.getDisplayName(entity.type, entity), entity },
                        ] as const,
                ),
            ]),
        [rememberedOptions, entities, entityRegistry],
    );
}
