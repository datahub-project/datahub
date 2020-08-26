import { IOwner } from '@datahub/data-models/types/entity/dataset/ownership';
import { OwnerUrnNamespace } from '@datahub/data-models/constants/entity/dataset/ownership';

export default [
  {
    confirmedBy: 'test',
    email: 'confirmed-owner@example.com',
    idType: 'USER',
    isActive: true,
    isGroup: true,
    modifiedTime: Date.now(),
    name: 'confirmed owner',
    userName: 'fakeconfirmedowner',
    namespace: OwnerUrnNamespace.corpUser,
    source: 'USER_INTERFACE',
    subType: null,
    type: 'DATA_OWNER'
  },
  {
    confirmedBy: 'test',
    email: 'suggested-owner@example.com',
    idType: 'USER',
    isActive: true,
    isGroup: true,
    modifiedTime: Date.now(),
    name: 'suggested owner',
    userName: 'fakesuggestedowner',
    namespace: OwnerUrnNamespace.corpUser,
    source: 'NUAGE',
    subType: null,
    type: 'DATA_OWNER'
  }
] as Array<IOwner>;
