import { Checkbox } from 'antd';
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

const StyledCheckbox = styled(Checkbox)`
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

    return (
        <div>
            <Checkbox.Group
                style={{ width: '200px' }}
                defaultValue={stagedSchemaFilterTypes}
                onChange={(values) => setStagedSchemaFilterTypes(values as SchemaFilterType[])}
            >
                <span>
                    <StyledCheckbox value={SchemaFilterType.FieldPath}>{tl('name')}</StyledCheckbox>
                </span>
                <span>
                    <StyledCheckbox value={SchemaFilterType.Documentation}>{t('tab.documentation')}</StyledCheckbox>
                </span>
                <span>
                    <StyledCheckbox value={SchemaFilterType.Tags}>{tl('tags')}</StyledCheckbox>
                </span>
                <span>
                    <StyledCheckbox value={SchemaFilterType.Terms}>{t('glossaryTerm.namePlural')}</StyledCheckbox>
                </span>
            </Checkbox.Group>
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
