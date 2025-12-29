// Shared URNs and constants for deploy-test lineage tests

export const PET_DETAILS_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)";

export const LONG_TAIL_PETS_URN =
  "urn:li:dataset:(urn:li:dataPlatform:looker,long_tail_companions.explore.long_tail_pets,PROD)";

export const LONG_TAIL_PETS_BREED_FIELD_URN =
  "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:looker,long_tail_companions.explore.long_tail_pets,PROD),dog_breeds.breed)";

export const DOG_BREEDS_VIEW_URN =
  "urn:li:dataset:(urn:li:dataPlatform:looker,long_tail_companions.view.dog_breeds,PROD)";

export const PET_DETAILS_VIEW_URN =
  "urn:li:dataset:(urn:li:dataPlatform:looker,long_tail_companions.view.pet_details,PROD)";
