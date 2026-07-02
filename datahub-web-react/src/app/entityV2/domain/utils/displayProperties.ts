import { hasLazyIcon } from '@app/mfeframework/lazyIconNames';

import { DisplayProperties, IconLibrary } from '@types';

// Shape of the `input` argument sent to `updateDisplayProperties` for a domain.
// Kept in sync with the DisplayPropertiesUpdateInput schema — both fields are optional
// so callers can update color-only or icon-only without touching the other.
export type DomainDisplayPropertiesInput = {
    colorHex?: string;
    icon?: {
        iconLibrary: IconLibrary;
        name: string;
        style: string;
    };
};

// `IconLibrary.Material` + `style: 'Outlined'` is a historical constant: the enum was
// coined for the pre-Phosphor MUI icon library and is still what the GMS resolver
// expects. We continue to write it for every icon (Phosphor picks included) because
// the enum currently has only one value and migrating it would be a breaking model
// change. Only the `icon.name` field carries meaningful state — post-migration writes
// always store a Phosphor name. Any lingering MUI names on legacy domains can be
// rewritten with the operator script at
// `metadata-ingestion/examples/library/domain_migrate_mui_icons_to_phosphor.py`.
const LEGACY_ICON_LIBRARY = IconLibrary.Material;
const LEGACY_ICON_STYLE = 'Outlined';

/**
 * Builds the input object for `updateDisplayProperties` from a domain edit/create form.
 *
 * Returns `null` when neither field is set — signaling the caller to skip the mutation
 * entirely rather than sending an empty update (which would still round-trip to GMS and
 * count against server-side rate limits).
 *
 * Empty string is treated as "unset" for both fields so callers can pass staged form
 * state directly without pre-filtering falsy values.
 */
export function buildDomainDisplayInput(fields: {
    colorHex?: string;
    iconName?: string;
}): DomainDisplayPropertiesInput | null {
    const colorHex = fields.colorHex || undefined;
    const iconName = fields.iconName || undefined;
    if (!colorHex && !iconName) return null;
    const input: DomainDisplayPropertiesInput = {};
    if (colorHex) {
        input.colorHex = colorHex;
    }
    if (iconName) {
        input.icon = {
            iconLibrary: LEGACY_ICON_LIBRARY,
            name: iconName,
            style: LEGACY_ICON_STYLE,
        };
    }
    return input;
}

/**
 * Minimal shape `resolveDomainEntityColor` needs — works for a full `Domain` object, the
 * `entityData` we pass through the entity header, and any partial fragment that carries a URN.
 */
export type DomainColorInput = {
    urn?: string | null;
    displayProperties?: Pick<DisplayProperties, 'colorHex'> | null;
};

/**
 * Canonical resolver for the hex color that visually represents a domain — used by the
 * profile header, the colored avatar, and the edit modal so every surface agrees.
 *
 * Priority (highest → lowest):
 *   1. `displayProperties.colorHex` set explicitly via the color picker
 *   2. A palette slot deterministically derived from the URN — so a domain without a saved
 *      color still has a stable visual identity across sessions
 *
 * Pure function — pass in the result of `useGenerateDomainColorFromPalette()` so it can be
 * exercised from component render and from unit tests. Mirrors `resolveGlossaryEntityColor`.
 */
export function resolveDomainEntityColor(entity: DomainColorInput, generateColor: (urn: string) => string): string {
    if (entity.displayProperties?.colorHex) {
        return entity.displayProperties.colorHex;
    }
    return generateColor(entity.urn || '');
}

export type DomainIconDisplay = {
    /** The Phosphor icon name to render, or empty string when we fall back to the letter avatar. */
    iconName: string;
    /** Convenience flag — `true` when the caller should render an icon, `false` for the letter fallback. */
    showIcon: boolean;
};

/**
 * Resolves a stored icon name for display, guarding against rendering an `AppWindow`
 * fallback for names that aren't in the loadable Phosphor set — in that case we return
 * `showIcon: false` so the caller can render the letter avatar and the domain's initial
 * stays visible.
 *
 * New writes always store a Phosphor name, so this is a straight pass-through +
 * loadability check. Legacy MUI names (from installations that have not yet run the
 * operator migration script at
 * `metadata-ingestion/examples/library/domain_migrate_mui_icons_to_phosphor.py`) will
 * fail the `hasLazyIcon` check and cleanly fall back to the letter avatar — the same
 * behavior we surface for a domain that never had an icon.
 */
export function resolveDomainIconDisplay(storedIconName: string | null | undefined): DomainIconDisplay {
    const iconName = storedIconName || '';
    const showIcon = !!iconName && hasLazyIcon(iconName);
    return { iconName, showIcon };
}

export type DomainEditInitialFields = {
    name: string;
    colorHex: string;
    /** The Phosphor-resolved (i.e. displayed) icon name — NOT the raw stored MUI name. */
    displayedIconName: string;
};

export type DomainEditStagedFields = {
    /** Post-trim staged name. Callers should `stagedName.trim()` before passing. */
    trimmedName: string;
    colorHex: string;
    iconName: string;
};

export type DomainEditFieldChanges = {
    nameChanged: boolean;
    colorChanged: boolean;
    iconChanged: boolean;
};

/**
 * Detects which fields the user actually changed in the edit-domain form. The icon
 * comparison intentionally uses the **displayed** (Phosphor-resolved) name — not the raw
 * stored MUI name — so silently opening and closing the modal never rewrites a legacy
 * `AccountCircle` aspect into `UserCircle`. We only persist an icon when the user
 * actively picked something different from what they saw.
 */
export function getDomainEditFieldChanges(
    initial: DomainEditInitialFields,
    staged: DomainEditStagedFields,
): DomainEditFieldChanges {
    return {
        nameChanged: staged.trimmedName !== initial.name,
        colorChanged: staged.colorHex !== initial.colorHex,
        iconChanged: staged.iconName !== initial.displayedIconName,
    };
}

// Read-side `displayProperties` shape used by `ListDomainFragment` (the sidebar, autocomplete,
// preview cards). Structurally compatible with the mutation input's icon shape but nullable
// and tagged with the __typenames Apollo expects.
export type OptimisticDomainDisplayProperties = {
    __typename: 'DisplayProperties';
    colorHex: string | null;
    icon: {
        __typename: 'IconProperties';
        iconLibrary: IconLibrary;
        name: string;
        style: string;
    } | null;
};

/**
 * Assembles the optimistic `displayProperties` we inject into the sidebar / domains context
 * after a create or edit.
 *
 * Motivation — the `updateDisplayProperties` mutation returns only a boolean, so Apollo can't
 * normalize a color/icon change into the `listDomains` cache. Without an explicit optimistic
 * fragment, the sidebar renders the letter-avatar + palette-from-URN fallback until a full
 * page refresh, even though the profile page (which refetches directly) is already correct.
 *
 * Composes on top of `buildDomainDisplayInput` so both sides agree on which fields count as
 * "set" (empty string = unset) and how the icon shape is populated with the legacy
 * `IconLibrary.Material` / `style: 'Outlined'` constants.
 */
export function buildOptimisticDomainDisplayProperties(fields: {
    colorHex?: string;
    iconName?: string;
}): OptimisticDomainDisplayProperties | null {
    const input = buildDomainDisplayInput(fields);
    if (!input) return null;
    return {
        __typename: 'DisplayProperties',
        colorHex: input.colorHex ?? null,
        icon: input.icon ? { __typename: 'IconProperties', ...input.icon } : null,
    };
}
