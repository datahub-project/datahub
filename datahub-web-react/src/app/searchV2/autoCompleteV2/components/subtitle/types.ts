import { FontColorLevelOptions, FontColorOptions } from '@components/theme/config';

import { Entity } from '@src/types.generated';

export interface EntitySubtitleProps {
    entity: Entity;
    color?: FontColorOptions;
    colorLevel?: FontColorLevelOptions;
}
