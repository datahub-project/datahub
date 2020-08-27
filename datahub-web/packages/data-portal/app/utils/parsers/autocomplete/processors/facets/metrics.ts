import { localFacetProcessor } from 'datahub-web/utils/parsers/autocomplete/processors/facets/local-facet';

const frequencies: Array<Com.Linkedin.Metric.MetricFrequencyType> = [
  'DAILY',
  'HOURLY',
  'MONTHLY',
  'REALTIME',
  'WEEKLY'
];

export const tier = localFacetProcessor('tier', ['0', '1', '2']);
export const pii = localFacetProcessor('pii', ['true', 'false']);
export const frequency = localFacetProcessor('frequency', frequencies);
export const highPriority = localFacetProcessor('highPriority', ['true', 'false']);
