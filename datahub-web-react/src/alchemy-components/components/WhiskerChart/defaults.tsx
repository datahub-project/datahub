import React from 'react';
import GlyphWithLineAndPopover from './components/GlyphWithLineAndPopover';
import WhiskerRenderer from './components/WhiskerRenderer';
import { DEFAULT_BOX_SIZE, DEFAULT_GAP_BETWEEN_WHISKERS } from './constants';
import { WhiskerChartProps } from './types';

export const whiskerChartDefaults: Omit<WhiskerChartProps, 'data'> = {
    boxSize: DEFAULT_BOX_SIZE,
    gap: DEFAULT_GAP_BETWEEN_WHISKERS,
    renderTooltip: (props) => <GlyphWithLineAndPopover {...props} />,
    renderWhisker: (props) => <WhiskerRenderer {...props} />,
};
