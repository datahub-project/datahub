import { resolvePhosphorIconName } from '@app/sharedV2/icons/materialToPhosphorIcon';

import { IconLibrary } from '@types';

/**
 * Resolve a stored displayProperties.icon.name to a Phosphor component name.
 * Phosphor library values pass through; Material (and unknown/legacy) names are mapped.
 * Returns null when no name is stored.
 */
export function resolveDisplayIconName(
    storedName: string | null | undefined,
    iconLibrary?: IconLibrary | string | null,
): string | null {
    if (!storedName?.trim()) {
        return null;
    }
    const name = storedName.trim();
    if (iconLibrary === IconLibrary.Phosphor || iconLibrary === 'PHOSPHOR') {
        return name;
    }
    return resolvePhosphorIconName(name);
}
