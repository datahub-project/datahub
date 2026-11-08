import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SchemaFilterType } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';
import { Checkbox, Menu, Text } from '@src/alchemy-components';
import { ItemType } from '@src/alchemy-components/components/Menu/types';
import { spacing } from '@src/alchemy-components/theme';

const OptionRow = styled.div`
    display: flex;
    align-items: center;
    gap: ${spacing.xxsm};
    padding: ${spacing.xxsm} ${spacing.xsm} ${spacing.xxsm} ${spacing.xxsm};
    min-width: 200px;
`;

type Props = {
    schemaFilterTypes: SchemaFilterType[];
    setSchemaFilterTypes: (filters: SchemaFilterType[]) => void;
    open: boolean;
    setOpen: (open: boolean) => void;
    children: React.ReactNode;
};

export default function SchemaFilterMenu({ schemaFilterTypes, setSchemaFilterTypes, open, setOpen, children }: Props) {
    const { t } = useTranslation('entity.types');
    const { t: tl } = useTranslation('common.labels');

    const options: { value: SchemaFilterType; label: string }[] = [
        { value: SchemaFilterType.FieldPath, label: tl('name') },
        { value: SchemaFilterType.Documentation, label: t('tab.documentation') },
        { value: SchemaFilterType.Tags, label: tl('tags') },
        { value: SchemaFilterType.Terms, label: t('glossaryTerm.namePlural') },
    ];

    const toggle = (value: SchemaFilterType) => {
        setSchemaFilterTypes(
            schemaFilterTypes.includes(value)
                ? schemaFilterTypes.filter((type) => type !== value)
                : [...schemaFilterTypes, value],
        );
    };

    const items: ItemType[] = options.map(({ value, label }) => ({
        type: 'item',
        key: value,
        title: label,
        // Stop the click before antd's menu sees it, so toggling an option keeps the menu open.
        render: () => (
            <OptionRow
                onClick={(e) => {
                    e.stopPropagation();
                    toggle(value);
                }}
            >
                <Checkbox isChecked={schemaFilterTypes.includes(value)} onCheckboxChange={() => toggle(value)} />
                <Text>{label}</Text>
            </OptionRow>
        ),
    }));

    return (
        <Menu items={items} open={open} onOpenChange={setOpen} trigger={['click']} placement="bottomRight">
            {children}
        </Menu>
    );
}
