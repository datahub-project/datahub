import { DefaultTheme, useTheme } from 'styled-components';

import { DisplayProperties, GlossaryNode, GlossaryTerm, ParentNodesResult } from '@types';

/**
 * Minimal shape `resolveGlossaryEntityColor` needs to operate on. Accepts anything that carries
 * the URN plus the optional fields used by the precedence chain — works for `GlossaryTerm`,
 * `GlossaryNode`, the GraphQL fragments, and the optimistic-entry shape we stash in
 * `nodeToNewEntity`.
 */
export type GlossaryEntityColorInput = {
    urn: string;
    displayProperties?: Pick<DisplayProperties, 'colorHex'> | null;
    parentNodes?: ParentNodesResult | null;
};

type ColorPalette = { [key: string]: string };

const getGlossaryV2PaletteColors = (theme: DefaultTheme): ColorPalette => ({
    PASTEL_LAVENDER: theme.colors.glossaryPaletteViolet,
    PURPLE: theme.colors.glossaryPalettePurple,
    LIGHT_BLUE: theme.colors.glossaryPaletteLightBlue,
    BLUE: theme.colors.glossaryPaletteBlue,
    FRESH_TEAL: theme.colors.glossaryPaletteTeal,
    GREEN: theme.colors.glossaryPaletteGreen,
    GREENISH_LIME: theme.colors.glossaryPaletteLime,
    LIGHT_ORANGE: theme.colors.glossaryPaletteLightOrange,
    PASTEL_MUSTARD: theme.colors.glossaryPaletteMustard,
    ORANGE: theme.colors.glossaryPaletteOrange,
    PEACH_ORANGE: theme.colors.glossaryPalettePeach,
    RED: theme.colors.glossaryPaletteRed,
    PASTEL_MAGENTA: theme.colors.glossaryPaletteMagenta,
    COLD_GREY: theme.colors.glossaryPaletteColdGrey,
});

export const getStringHash = (str: string) => {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        /* eslint-disable no-bitwise */
        hash = str.charCodeAt(i) + ((hash << 5) - hash);
    }
    return hash;
};

export function useGenerateGlossaryColorFromPalette() {
    const theme = useTheme();

    const generateColor = (urn: string) => {
        const palette = theme?.colors?.glossaryPalette || Object.values(getGlossaryV2PaletteColors(theme));
        const colorIndex = Math.abs(getStringHash(urn)) % palette.length;
        return palette[colorIndex];
    };

    return generateColor;
}

/**
 * Picks the deepest (last) parent node from a term's parent-nodes list.
 *
 * The GraphQL `parentNodes` array is ordered direct-parent → root, so the last entry
 * is the deepest ancestor (i.e. the root glossary node). Returns `null` when the term
 * has no parents (root-level term).
 */
export function getDeepestParentNode(parentNodes: ParentNodesResult | null | undefined): GlossaryNode | null {
    if (!parentNodes || parentNodes.count === 0) return null;
    return parentNodes.nodes[parentNodes.count - 1] as GlossaryNode;
}

/**
 * Canonical resolver for the hex color used to visually represent a glossary entity (term or
 * node) across the entire app — sidebar, entity header, list cards, autocomplete, modal pickers.
 *
 * Priority (highest → lowest):
 *   1. The entity's own `displayProperties.colorHex` — set explicitly via the color picker. A
 *      user-picked color is always the source of truth.
 *   2. An explicit `inheritedColor` — used by the sidebar tree where each `NodeItem` passes its
 *      resolved color down to its children so a deeply-nested descendant of "Adoption" reads the
 *      same pink bookmark icon as Adoption itself. Skip this argument anywhere the resolution is
 *      not happening inside a recursive render (e.g. flat list cards, autocomplete).
 *   3. The deepest (root) parent node's color, derived from its own `colorHex` or a palette
 *      slot seeded by its URN. Gives any descendant the visual identity of its group.
 *   4. For root-level entities with no parents, a palette slot derived from the entity's URN.
 *
 * Pure function — pass in the result of `useGenerateGlossaryColorFromPalette()` so it can be
 * exercised from component render and from unit tests.
 */
export function resolveGlossaryEntityColor(
    entity: GlossaryEntityColorInput,
    generateColor: (urn: string) => string,
    options?: { inheritedColor?: string | null },
): string {
    if (entity.displayProperties?.colorHex) {
        return entity.displayProperties.colorHex;
    }
    if (options?.inheritedColor) {
        return options.inheritedColor;
    }
    const parent = getDeepestParentNode(entity.parentNodes);
    if (parent) {
        return parent.displayProperties?.colorHex || generateColor(parent.urn);
    }
    return generateColor(entity.urn);
}

/**
 * Back-compat wrapper around {@link resolveGlossaryEntityColor} kept for callers that still
 * import the term-specific signature. Prefer `resolveGlossaryEntityColor` for new code — it
 * works for both terms and nodes and accepts an `inheritedColor` for the sidebar's recursive
 * render.
 */
export function getGlossaryTermColor(
    term: Pick<GlossaryTerm, 'urn' | 'parentNodes' | 'displayProperties'>,
    generateColor: (urn: string) => string,
): string {
    return resolveGlossaryEntityColor(term, generateColor);
}
