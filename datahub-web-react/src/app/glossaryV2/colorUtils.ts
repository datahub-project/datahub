import { DefaultTheme, useTheme } from 'styled-components';

import { GlossaryNode, GlossaryTerm, ParentNodesResult } from '@types';

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
 * Returns the hex color used to visually represent a glossary term across the app.
 *
 * Convention: a term inherits the color of its deepest (root) parent node. If that node
 * has a configured `displayProperties.colorHex`, that wins. Otherwise we deterministically
 * derive a palette color from the parent's URN. For root-level terms (no parent nodes),
 * we fall back to a palette color derived from the term's own URN.
 *
 * Pure function — pass in the result of `useGenerateGlossaryColorFromPalette()` so it
 * can be exercised from component render and from unit tests.
 */
export function getGlossaryTermColor(
    term: Pick<GlossaryTerm, 'urn' | 'parentNodes'>,
    generateColor: (urn: string) => string,
): string {
    const parent = getDeepestParentNode(term.parentNodes);
    if (parent) {
        return parent.displayProperties?.colorHex || generateColor(parent.urn);
    }
    return generateColor(term.urn);
}
