/**
 * Reflection of the CorpUserEditableInfo modeling on the backend
 */
export interface ICorpUserEditableInfo {
  // A person's self derived bio
  aboutMe: string;
  // A list of tags that represent the self-assigned team of the person
  teams: Array<string>;
  // A self-assigned list of skills that the person claims to own
  skills: Array<string>;
}

/**
 * Expected response for a person entity readEntity() call. This is a reflection of the
 * CorpUserInfo pdsc modeling on the backend
 */
export interface ICorpUserInfo {
  // Identifying username for the user. Should be unique to the particular person
  username: string;
  info: {
    // Whether the person is active in the company or not
    active: boolean;
    // Read-friendly name to display for the person
    displayName: string;
    // Email address for the particular person
    email: string;
    // The person's title/position within the company
    title: string;
    // Identifier for the person's manager by their urn
    // Nullable if the person has no manager
    managerUrn: string | null;
    // Given name for the person's manager, returned from the API as a convenience
    managerName: string | null;
    // A numerical identifier for the person's department
    departmentId: number;
    // A human read-friendly identifier for the person's department
    departmentName: string;
    // Broken down first name for the person
    firstName: string;
    // Broken down last name for the person
    lastName: string;
    // The person's full name
    fullName: string;
    // An identifier for the person's current country by some code
    countryCode: string;
  };
  editableInfo?: ICorpUserEditableInfo;
  datasetRecommendationsInfo?: Com.Linkedin.Identity.DatasetRecommendationsInfo;
}
