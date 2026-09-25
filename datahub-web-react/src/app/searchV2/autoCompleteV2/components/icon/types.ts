import { Entity } from '@src/types.generated';

export interface EntityIconProps {
    entity: Entity;
    siblings?: Entity[];
    /** Overrides the icon's rendered size in px. Falls back to each icon type's own default when omitted. */
    size?: number;
}
