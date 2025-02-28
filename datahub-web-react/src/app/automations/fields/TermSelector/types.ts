import { EntityType } from '@src/types.generated';

export type RadioValue = 'all' | 'some' | 'none';

export type SelectDropdownProps = {
    label: string;
    type: EntityType;
    placeholder?: string;
    preselectedOptions: string[];
    enabled?: boolean;
    onChange?: (value: any) => void;
};
