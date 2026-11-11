import { Entity } from '@src/types.generated';

export interface EntityIconProps {
    entity: Entity;
    siblings?: Entity[];
    /** Overrides the icon's rendered size in px. Falls back to each icon type's own default when omitted. */
    size?: number;
    /**
     * When true, never stacks sibling/multi-platform logos — show only the primary entity icon.
     * Prefer leaving this unset: stacked logos scale down to fit `size` instead of clipping.
     */
    preferSingleIcon?: boolean;
}
