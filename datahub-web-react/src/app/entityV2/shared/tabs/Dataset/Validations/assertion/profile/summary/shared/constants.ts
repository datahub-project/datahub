import { AssertionStdOperator } from '@types';

/* eslint-disable i18next/no-literal-string -- Return value is a sentence fragment inserted mid-sentence by callers;
   verb agreement and word order depend on the surrounding sentence and cannot be determined from the fragment alone */
export const GET_ASSERTION_OPERATOR_TO_DESCRIPTION_MAP = ({ isPlural }) => ({
    [AssertionStdOperator.EqualTo]: `${isPlural ? 'are' : 'Is'} equal to`,
    [AssertionStdOperator.NotEqualTo]: `${isPlural ? 'are' : 'Is'} not equal to`,
    [AssertionStdOperator.Contain]: 'Contains',
    [AssertionStdOperator.RegexMatch]: 'Matches',
    [AssertionStdOperator.StartWith]: 'Starts with',
    [AssertionStdOperator.EndWith]: 'Ends with',
    [AssertionStdOperator.In]: `${isPlural ? 'are' : 'Is'} in`,
    [AssertionStdOperator.NotIn]: `${isPlural ? 'are' : 'Is'} not in`,
    [AssertionStdOperator.IsFalse]: `${isPlural ? 'are' : 'Is'} False`,
    [AssertionStdOperator.IsTrue]: `${isPlural ? 'are' : 'Is'} True`,
    [AssertionStdOperator.Null]: `${isPlural ? 'are' : 'Is'} NULL`,
    [AssertionStdOperator.NotNull]: `${isPlural ? 'are' : 'Is'} not NULL`,
    [AssertionStdOperator.GreaterThan]: `${isPlural ? 'are' : 'Is'} greater than`,
    [AssertionStdOperator.GreaterThanOrEqualTo]: `${isPlural ? 'are' : 'Is'} greater than or equal to`,
    [AssertionStdOperator.LessThan]: `${isPlural ? 'are' : 'Is'} less than`,
    [AssertionStdOperator.LessThanOrEqualTo]: `${isPlural ? 'are' : 'Is'} less than or equal to`,
    [AssertionStdOperator.Between]: `${isPlural ? 'are' : 'Is'} within a range`,
    [AssertionStdOperator.Native]: undefined,
});
/* eslint-enable i18next/no-literal-string */
