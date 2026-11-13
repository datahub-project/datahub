import { Container, Entity, EntityType } from '@types';

interface OptionWithEntity {
    value: string;
    entity?: Entity | null;
}

type GetLabel = (entity: Entity) => string;

const getParentChain = (entity: Entity): Entity[] => (entity as Container).parentContainers?.containers || [];

const toKey = (label: string): string => label.trim().toLowerCase();

/**
 * Container filter options often share a name: a "store" schema can exist in several databases, so
 * the option list would show several identical "store" entries. For each group of container options
 * whose labels collide, find the fewest parent levels (nearest parent first, so the database before
 * any higher level) that make the group's labels unique, and return those parents per option value.
 * Options with a unique label are left out, so the list only grows where it has to. If no depth tells
 * a group apart (the same names all the way up), every option in it gets its whole parent chain.
 *
 * Parents are returned nearest-first, the order `parentContainers` uses.
 */
export function getContainerDisambiguationParents(
    options: OptionWithEntity[],
    getLabel: GetLabel,
): Map<string, Entity[]> {
    const groups = new Map<string, OptionWithEntity[]>();
    options.forEach((option) => {
        if (option.entity?.type !== EntityType.Container) return;
        const key = toKey(getLabel(option.entity));
        groups.set(key, [...(groups.get(key) || []), option]);
    });

    const result = new Map<string, Entity[]>();
    groups.forEach((group) => {
        if (group.length < 2) return;
        const chains = group.map((option) => getParentChain(option.entity as Entity));
        const maxDepth = Math.max(...chains.map((chain) => chain.length));
        const pathKey = (chain: Entity[], depth: number) =>
            chain
                .slice(0, depth)
                .map((parent) => toKey(getLabel(parent)))
                .join('/');

        let depth = maxDepth;
        for (let candidate = 1; candidate < maxDepth; candidate++) {
            const keys = new Set(chains.map((chain) => pathKey(chain, candidate)));
            if (keys.size === chains.length) {
                depth = candidate;
                break;
            }
        }

        group.forEach((option, index) => {
            const parents = chains[index].slice(0, depth);
            if (parents.length) result.set(option.value, parents);
        });
    });
    return result;
}

/** A dotted path from the outermost of `parents` (nearest-first) down to `label`: "petshop_east.store". */
export function formatContainerPath(label: string, parents: Entity[], getLabel: GetLabel): string {
    return [...parents].reverse().map(getLabel).concat(label).join('.');
}

/** The container's full path, from its outermost parent container down to the container itself. */
export function getFullContainerPath(entity: Entity, getLabel: GetLabel): string {
    return formatContainerPath(getLabel(entity), getParentChain(entity), getLabel);
}

/** One piece of a displayed path. `hidden` is the text an ellipsis in `text` stands for, if any. */
export interface ContainerPathSegment {
    key: string;
    text: string;
    hidden?: string;
}

// Roughly what fits in the option row next to the checkbox and count.
export const MAX_PATH_CHARS = 32;
export const MAX_SEGMENT_CHARS = 14;

const ELLIPSIS = '\u2026';

// Cut from the middle: same-named containers usually differ at the start or the end of a name
// ("..._2019_to_2024" vs "..._2014_to_2018"), so both ends stay visible.
const shorten = (key: string, name: string): ContainerPathSegment => {
    if (name.length <= MAX_SEGMENT_CHARS) return { key, text: name };
    const head = Math.floor((MAX_SEGMENT_CHARS - 1) / 2);
    const tail = Math.ceil((MAX_SEGMENT_CHARS - 1) / 2);
    return { key, text: `${name.slice(0, head)}${ELLIPSIS}${name.slice(name.length - tail)}`, hidden: name };
};

/**
 * The path to display for `label` with its disambiguating `parents` (nearest-first). Short paths are
 * shown whole. A long one keeps the two parts that matter, the outermost parent (the one that tells
 * same-named options apart) and the container's own name, collapses the levels between them to an
 * ellipsis, and shortens over-long names in the middle. Each ellipsis records what it hides.
 */
export function getContainerPathSegments(label: string, parents: Entity[], getLabel: GetLabel): ContainerPathSegment[] {
    const outerFirst = [...parents].reverse();
    const names = outerFirst.map(getLabel).concat(label);
    if (names.join('.').length <= MAX_PATH_CHARS) {
        return outerFirst
            .map((parent) => ({ key: parent.urn, text: getLabel(parent) }))
            .concat({ key: 'self', text: label });
    }

    const middle = names.slice(1, -1);
    return [
        shorten(outerFirst[0].urn, names[0]),
        ...(middle.length ? [{ key: 'collapsed', text: ELLIPSIS, hidden: middle.join('.') }] : []),
        shorten('self', label),
    ];
}
