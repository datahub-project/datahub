// these relationships trigger a link where the source is to the right of the sink
export const FORWARD_RELATIONSHIPS = ['DownstreamOf', 'Consumes', 'Contains', 'TrainedBy'];
// these relationships trigger a link where the source is to the left of the sink
export const INVERSE_RELATIONSHIPS = ['Produces', 'MemberOf'];

export const HORIZONTAL_SPACE_PER_LAYER = 400;
export const VERTICAL_SPACE_BETWEEN_NODES = 40;

export const CURVE_PADDING = 75;
