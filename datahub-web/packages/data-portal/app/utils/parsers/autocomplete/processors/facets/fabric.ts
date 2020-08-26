import { localFacetProcessor } from 'datahub-web/utils/parsers/autocomplete/processors/facets/local-facet';
import { fabricsArray } from '@datahub/data-models/entity/dataset/utils/urn';

/**
 * when we expect to auto suggest a fabric, we just do a local search for the fabrics available
 */
export const fabric = localFacetProcessor('dataorigin', fabricsArray);
