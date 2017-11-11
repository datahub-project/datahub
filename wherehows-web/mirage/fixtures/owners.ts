import { IOwner } from 'wherehows-web/typings/api/datasets/owners';
import { OwnerSource, OwnerIdType, OwnerUrnNamespace } from 'wherehows-web/utils/api/datasets/owners';

export default <Array<IOwner>>[
  {
    confirmedBy: '',
    email: 'confirmed-owner@linkedin.com',
    idType: OwnerIdType.User,
    isActive: true,
    isGroup: true,
    modifiedTime: Date.now(),
    name: 'confirmed owner',
    userName: 'fakeconfirmedowner',
    namespace: OwnerUrnNamespace.corpUser,
    source: OwnerSource.Ui,
    subType: null,
    type: 'Owner'
  },
  {
    confirmedBy: '',
    email: 'suggested-owner@linkedin.com',
    idType: OwnerIdType.User,
    isActive: true,
    isGroup: true,
    modifiedTime: Date.now(),
    name: 'suggested owner',
    userName: 'fakesuggestedowner',
    namespace: OwnerUrnNamespace.corpUser,
    source: OwnerSource.Nuage,
    subType: null,
    type: 'Owner'
  }
];
