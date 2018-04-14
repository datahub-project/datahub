/**
 * A list of mock field change sets depicting the different scenarios
 * @type {Array<{isDirty: boolean; privacyPolicyExists: boolean; suggestionAuthority: boolean; suggestion?: {}; __requiresReview__: boolean; __msg__: string}>
 */
const suggestion = { identifierType: 'same' };
const mockFieldChangeSets = [
  {
    isDirty: true,
    privacyPolicyExists: true,
    suggestionAuthority: false,
    suggestion,
    identifierType: 'other',
    __requiresReview__: true,
    __msg__: 'Suggestion exists but user has not affirmed or ignored suggestion (suggestionAuthority)'
  },
  {
    isDirty: true,
    privacyPolicyExists: false,
    suggestionAuthority: true,
    __requiresReview__: false,
    __msg__: 'NO suggestion, and policy does NOT exist and user has made changes to local working copy'
  },
  {
    isDirty: false,
    privacyPolicyExists: false,
    suggestionAuthority: true,
    __requiresReview__: true,
    __msg__: 'NO suggestion, and policy does NOT exist but user has NOT made changes to local working copy'
  },
  {
    isDirty: true,
    privacyPolicyExists: false,
    suggestionAuthority: true,
    suggestion,
    identifierType: 'same',
    __requiresReview__: false,
    __msg__: 'NO suggestion, and policy does NOT exist but user has NOT made changes to local working copy'
  }
];

export { mockFieldChangeSets };
