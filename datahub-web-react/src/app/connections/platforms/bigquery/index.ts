import { buildComponents } from '@app/connections/factory';
import * as constants from '@app/connections/platforms/bigquery/constants';

// Build components with the constants
const components = buildComponents(constants);

// Export the components and constants/config
export { components, constants };
