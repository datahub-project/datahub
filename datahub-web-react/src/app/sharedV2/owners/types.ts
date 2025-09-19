import { SelectOption } from '@components';

import { ExtendedOwner } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/ownershipUtils';

// FYI: associatedUrn is required in the owner type but in case of the creation of the new source it's undefined until the source created
export type PartialExtendedOwner = Omit<ExtendedOwner, 'associatedUrn'> & Partial<Pick<ExtendedOwner, 'associatedUrn'>>;

export interface OwnerSelectOption extends SelectOption {
    owner: PartialExtendedOwner;
}
