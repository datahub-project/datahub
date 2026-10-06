import type { Icon, IconWeight } from '@phosphor-icons/react';
import React from 'react';

import ColoredEntityIcon from '@app/sharedV2/icons/ColoredEntityIcon';

interface Props {
    color: string;
    icon: Icon;
    size?: number;
    iconSize?: number;
    /** Override the container's border-radius (defaults to `size / 4`). */
    radius?: number;
    /** Phosphor glyph weight. Cards use `regular`; denser surfaces keep the heavier default. */
    weight?: IconWeight;
    className?: string;
}

export default function GlossaryColoredIcon(props: Props) {
    return <ColoredEntityIcon {...props} />;
}
