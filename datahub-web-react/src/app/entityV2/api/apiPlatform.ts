import { Api } from '@types';

// Api.platform is nullable, so queries select it as `apiPlatform: platform` to avoid a GraphQL
// field-merge conflict with the non-null `platform` of other entity types in the same selection.
export type ApiWithPlatformAlias = Api & { apiPlatform?: Api['platform'] };

export function getApiPlatform(api?: ApiWithPlatformAlias | null): Api['platform'] {
    return api?.apiPlatform ?? api?.platform;
}
