import { IOwnerResponse } from '@datahub/data-models/types/entity/dataset/ownership';

/**
 * Helper method to map legacy ownership type to new ownership type
 * @param category The category which the owner belongs to
 */
const transformOwnerCategoryIntoType = (
  category: Com.Linkedin.Dataset.OwnerCategory
): Com.Linkedin.Common.OwnershipType => {
  switch (category) {
    case 'DATA_OWNER':
      return 'DEVELOPER';
      break;
    case 'PRODUCER':
    case 'CONSUMER':
    case 'DELEGATE':
    case 'STAKEHOLDER':
      return category;
      break;
  }
};

/**
 * Helper method that converts an legacy dataset owner response into an array of owners that is more baseEntity friendly.
 * @param ownersResponse The response from the api that contains the owner information for a dataset
 */
export const transformOwnersResponseIntoOwners = (ownersResponse: IOwnerResponse): Array<Com.Linkedin.Common.Owner> => {
  const { owners = [] } = ownersResponse;
  return owners.map(owner => ({
    owner: owner.userName,
    type: transformOwnerCategoryIntoType(owner.type)
  }));
};
