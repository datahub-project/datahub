import { serializeStringArray } from '@datahub/utils/array/serialize-string';
import { difference } from 'lodash';

/**
 * Validates that an array of json object string values match
 * @param {Array<string>} values the received list of strings
 * @param {Array<string>} expectedValues the expected list of strings
 * @returns {true}
 */
export const jsonValuesMatch = (values: Array<string>, expectedValues: Array<string>): true => {
  const sValues = serializeStringArray(values);
  const sExpectedValues = serializeStringArray(expectedValues);
  const match = sValues === sExpectedValues;

  if (!match) {
    throw new Error(
      ` Found ${difference(values, expectedValues).join(', ')}. Expected only ${expectedValues.join(', ')}`
    );
  }

  return match;
};

/**
 * Validates that an array of json object string values only contains what is expected. :)
 * @param values - received list of strings
 * @param expectedValues - expected list of strings
 */
export const jsonValuesAreIncluded = (values: Array<string>, expectedValues: Array<string>): true => {
  const serializedExpectedValues = serializeStringArray(expectedValues);

  values.forEach(value => {
    if (!serializedExpectedValues.includes(value)) {
      throw new Error(`Found unexpected values ${difference(values, expectedValues).join(', ')}`);
    }
  });

  return true;
};
