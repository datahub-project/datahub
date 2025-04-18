import { buildComponents } from '../../factory';
import * as constants from './constants';

// Build components with the constants
const components = buildComponents(constants);

// Export the components and constants/config
export { components, constants };
