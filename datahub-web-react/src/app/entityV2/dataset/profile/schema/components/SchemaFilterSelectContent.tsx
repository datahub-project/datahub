import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SchemaFilterType } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';
import { Checkbox, Text } from '@src/alchemy-components';
import { radius, spacing } from '@src/alchemy-components/theme';

type Props = {
    schemaFilterTypes: SchemaFilterType[];
    setSchemaFilterTypes: (filters: SchemaFilterType[]) => void;
};

const Container = styled.div`
    display: flex;
    flex-direction: column;
    min-width: 220px;
    padding: ${spacing.xsm};
`;

const OptionRow = styled.div`
    display: flex;
    align-items: center;
    gap: ${spacing.xxsm};
    padding: ${spacing.xxsm};
    border-radius: ${radius.md};
    cursor: pointer;

    :hover {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
`;

export default function SchemaFilterSelectContent({ schemaFilterTypes, setSchemaFilterTypes }: Props) {
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

    return (
        <Container>
            {options.map(({ value, label }) => (
                <OptionRow key={value} onClick={() => toggle(value)}>
                    <Checkbox isChecked={schemaFilterTypes.includes(value)} onCheckboxChange={() => toggle(value)} />
                    <Text size="md">{label}</Text>
                </OptionRow>
            ))}
        </Container>
    );
}
