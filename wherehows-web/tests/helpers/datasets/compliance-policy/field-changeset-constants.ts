/**
 * A list of mock field change sets depicting the different scenarios
 * @type {Array<{isDirty: boolean; privacyPolicyExists: boolean; suggestionAuthority: boolean; suggestion: boolean; __requiresReview__: boolean; __msg__: string}>
 */
const mockFieldChangeSets = [
  {
    isDirty: true,
    privacyPolicyExists: true,
    suggestionAuthority: false,
    suggestion: true,
    __requiresReview__: true,
    __msg__: 'Suggestion exists but user has not affirmed or ignored suggestion (suggestionAuthority)'
  },
  {
    isDirty: true,
    privacyPolicyExists: false,
    suggestionAuthority: true,
    suggestion: false,
    __requiresReview__: false,
    __msg__: 'NO suggestion, and policy does NOT exist and user has made changes to local working copy'
  },
  {
    isDirty: false,
    privacyPolicyExists: false,
    suggestionAuthority: true,
    suggestion: false,
    __requiresReview__: true,
    __msg__: 'NO suggestion, and policy does NOT exist but user has NOT made changes to local working copy'
  }
];

export { mockFieldChangeSets };
