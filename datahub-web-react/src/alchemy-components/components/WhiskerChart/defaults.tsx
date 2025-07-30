import React from 'react';

import GlyphWithLineAndPopover from '@components/components/WhiskerChart/components/GlyphWithLineAndPopover';
import WhiskerRenderer from '@components/components/WhiskerChart/components/WhiskerRenderer';
import { DEFAULT_BOX_SIZE, DEFAULT_GAP_BETWEEN_WHISKERS } from '@components/components/WhiskerChart/constants';
import { WhiskerChartProps } from '@components/components/WhiskerChart/types';

export const whiskerChartDefaults: Omit<WhiskerChartProps, 'data'> = {
    boxSize: DEFAULT_BOX_SIZE,
    gap: DEFAULT_GAP_BETWEEN_WHISKERS,
    renderTooltip: (props) => <GlyphWithLineAndPopover {...props} />,
    renderWhisker: (props) => <WhiskerRenderer {...props} />,
};
