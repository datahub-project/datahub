import { Entity } from '@types';

type SiblingsAware = {
    siblings?: {
        isPrimary?: boolean | null;
        siblings?: Array<{ urn?: string | null } | null> | null;
    } | null;
};

function getSiblingUrns(entity: Entity): string[] {
    const { siblings } = entity as Entity & SiblingsAware;
    return (siblings?.siblings ?? []).map((sibling) => sibling?.urn).filter((urn): urn is string => !!urn);
}

function isPrimarySibling(entity: Entity): boolean {
    return !!(entity as Entity & SiblingsAware).siblings?.isPrimary;
}

/**
 * Collapse sibling entities (e.g. a dbt model and the table it materializes) down to one entry
 * per sibling cohort, so compact cards don't render the same asset twice.
 *
 * Cohort membership is read from the `siblings` aspect urns on each result, which means this
 * needs no per-result `siblingsSearch`. That keeps the home-widget card query free of nested
 * searches, at the cost of not merging metadata across the cohort — compact cards render only
 * the icon, name and platform of a single entity, so there is nothing to merge.
 *
 * Cohorts keep the position of their highest-ranked member, and the primary sibling represents
 * the cohort when it is present in the same result set.
 */
export function collapseSiblingEntities(entities: Entity[]): Entity[] {
    const cohortIndexByUrn = new Map<string, number>();
    const cohorts: Entity[][] = [];

    entities.forEach((entity) => {
        const urns = [entity.urn, ...getSiblingUrns(entity)];
        const cohortIndex = urns.map((urn) => cohortIndexByUrn.get(urn)).find((index) => index !== undefined);

        if (cohortIndex === undefined) {
            cohorts.push([entity]);
            urns.forEach((urn) => cohortIndexByUrn.set(urn, cohorts.length - 1));
            return;
        }

        cohorts[cohortIndex].push(entity);
        urns.forEach((urn) => {
            if (!cohortIndexByUrn.has(urn)) cohortIndexByUrn.set(urn, cohortIndex);
        });
    });

    return cohorts.map((cohort) => cohort.find(isPrimarySibling) ?? cohort[0]);
}
