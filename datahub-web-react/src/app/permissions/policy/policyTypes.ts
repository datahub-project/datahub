import { PoliciesConfig } from '@types';

export type PolicyPrivilegesConfig = Pick<PoliciesConfig, 'platformPrivileges' | 'resourcePrivileges'>;
