import { Checkbox } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SchemaFilterType } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';
import { Button } from '@src/alchemy-components';

type Props = {
    schemaFilterTypes: SchemaFilterType[];
    setSchemaFilterTypes: (filters: SchemaFilterType[]) => void;
    close: () => void;
};

const OptionList = styled.div`
    display: flex;
    flex-direction: column;
    width: 200px;
`;

// The whole row toggles, like antd's checkbox label did; the box sits left of the text.
const FilterOption = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    cursor: pointer;
    font-size: 14px;
    line-height: 22px;
    padding-top: 5px;
    padding-bottom: 5px;
    margin-left: -16px;
    padding-left: 16px;
    :hover {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
    min-width: 232px;
`;

const StyledButton = styled(Button)`
    width: 100%;
    margin-top: 12px;
    display: flex;
    justify-content: center;
`;

export default function SchemaFilterSelectContent({ schemaFilterTypes, setSchemaFilterTypes, close }: Props) {
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const { t: tl } = useTranslation('common.labels');
    const [stagedSchemaFilterTypes, setStagedSchemaFilterTypes] = useState<SchemaFilterType[]>(schemaFilterTypes);

    const options = [
        { value: SchemaFilterType.FieldPath, label: tl('name') },
        { value: SchemaFilterType.Documentation, label: t('tab.documentation') },
        { value: SchemaFilterType.Tags, label: tl('tags') },
        { value: SchemaFilterType.Terms, label: t('glossaryTerm.namePlural') },
    ];

    const setOptionChecked = (value: SchemaFilterType, checked: boolean) => {
        setStagedSchemaFilterTypes((current) =>
            options
                .map((option) => option.value)
                .filter((optionValue) => (optionValue === value ? checked : current.includes(optionValue))),
        );
    };

    return (
        <div>
            <OptionList>
                {options.map((option) => {
                    const isChecked = stagedSchemaFilterTypes.includes(option.value);
                    return (
                        <FilterOption key={option.value} onClick={() => setOptionChecked(option.value, !isChecked)}>
                            <Checkbox
                                isChecked={isChecked}
                                onCheckboxChange={(checked) => setOptionChecked(option.value, checked)}
                            />
                            {option.label}
                        </FilterOption>
                    );
                })}
            </OptionList>
            <StyledButton
                onClick={() => {
                    setSchemaFilterTypes(stagedSchemaFilterTypes);
                    close();
                }}
            >
                {tc('apply')}
            </StyledButton>
        </div>
    );
}
