const suggestion = { identifierType: 'same', confidenceLevel: 0.6 };
/**
 * A list of mock field change sets depicting the different scenarios
 * @type {Array<{isDirty: boolean; privacyPolicyExists: boolean; suggestionAuthority: boolean; suggestion?: {}; __requiresReview__: boolean; __msg__: string}>
 */
const mockFieldChangeSets = [
  {
    isDirty: true,
    privacyPolicyExists: false,
    suggestionAuthority: true,
    __requiresReview__: true,
    __msg__:
      'No suggestion, and policy does NOT exist and user has made changes to local working copy, but no identifierType'
  },
  {
    isDirty: false,
    privacyPolicyExists: false,
    suggestionAuthority: false,
    __requiresReview__: true,
    __msg__: 'No identifierType present'
  },
  {
    isDirty: false,
    privacyPolicyExists: false,
    suggestionAuthority: true,
    __requiresReview__: true,
    __msg__: 'No suggestion, and policy does NOT exist but user has NOT made changes to local working copy'
  },
  {
    isDirty: true,
    privacyPolicyExists: false,
    suggestionAuthority: true,
    suggestion,
    identifierType: 'same',
    __requiresReview__: false,
    __msg__: 'Suggestion exists, and policy does NOT exist but user has made changes to local working copy'
  }
];

export { mockFieldChangeSets };
