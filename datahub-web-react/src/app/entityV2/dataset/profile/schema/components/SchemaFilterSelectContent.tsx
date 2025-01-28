import React, { useState } from 'react';
import { Button, Checkbox } from 'antd';
import styled from 'styled-components';

import { SchemaFilterType } from '../../../../shared/tabs/Dataset/Schema/utils/filterSchemaRows';
import { ANTD_GRAY } from '../../../../shared/constants';

type Props = {
    schemaFilterTypes: SchemaFilterType[];
    setSchemaFilterTypes: (filters: SchemaFilterType[]) => void;
    close: () => void;
};

const UpdateButton = styled(Button)`
    width: 100%;
    text-align: center;
    background-color: ${(props) => props.theme.styles['primary-color']};
    color: white;
    border-radius: 0;
    margin-top: 10px;
`;

const StyledCheckbox = styled(Checkbox)`
    font-size: 14px;
    line-height: 22px;
    padding-top: 5px;
    padding-bottom: 5px;
    margin-left: -16px;
    padding-left: 16px;
    :hover {
        background-color: ${ANTD_GRAY[3]};
    }
    width: 232px;
`;

export default function SchemaFilterSelectContent({ schemaFilterTypes, setSchemaFilterTypes, close }: Props) {
    const [stagedSchemaFilterTypes, setStagedSchemaFilterTypes] = useState<SchemaFilterType[]>(schemaFilterTypes);

    return (
        <div>
            <Checkbox.Group
                style={{ width: '200px' }}
                defaultValue={stagedSchemaFilterTypes}
                onChange={(values) => setStagedSchemaFilterTypes(values as SchemaFilterType[])}
            >
                <span>
                    <StyledCheckbox value={SchemaFilterType.FieldPath}>Name</StyledCheckbox>
                </span>
                <span>
                    <StyledCheckbox value={SchemaFilterType.Documentation}>Documentation</StyledCheckbox>
                </span>
                <span>
                    <StyledCheckbox value={SchemaFilterType.Tags}>Tags</StyledCheckbox>
                </span>
                <span>
                    <StyledCheckbox value={SchemaFilterType.Terms}>Glossary Terms</StyledCheckbox>
                </span>
            </Checkbox.Group>
            <div>
                <UpdateButton
                    onClick={() => {
                        setSchemaFilterTypes(stagedSchemaFilterTypes);
                        close();
                    }}
                >
                    Update
                </UpdateButton>
            </div>
        </div>
    );
}
