import { faker } from 'ember-cli-mirage';
import { dasherize } from '@ember/string';

export const getAutocompleteDatasets = function(_: any, request: any) {
  return {
    status: 'ok',
    input: request.queryParams.input,
    source: [...Array(10)].map(() => dasherize(faker.lorem.words().toLowerCase()))
  };
};
