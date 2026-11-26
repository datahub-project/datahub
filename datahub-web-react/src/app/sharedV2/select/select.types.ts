import { NestedSelectOption } from '@components/components/Select/Nested/types';

import { Entity } from '@types';

export type EntityNestedSelectOption = NestedSelectOption & {
    entity?: Entity;
};
