// these relationships trigger a link where the source is to the right of the sink
export const FORWARD_RELATIONSHIPS = ['DownstreamOf', 'Consumes', 'Contains', 'TrainedBy'];
// these relationships trigger a link where the source is to the left of the sink
export const INVERSE_RELATIONSHIPS = ['Produces', 'MemberOf'];

export const HORIZONTAL_SPACE_PER_LAYER = 400;
export const VERTICAL_SPACE_BETWEEN_NODES = 40;
export const EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT = 35;
export const NUM_COLUMNS_PER_PAGE = 10;
export const COLUMN_HEIGHT = 30;

export const CURVE_PADDING = 75;

export const width = 250;
export const height = 80;
export const iconWidth = 32;
export const iconHeight = 32;
export const iconX = -width / 2 + 22;
export const iconY = -iconHeight / 2;
export const centerX = -width / 2;
export const centerY = -height / 2;
export const textX = iconX + iconWidth + 8;
