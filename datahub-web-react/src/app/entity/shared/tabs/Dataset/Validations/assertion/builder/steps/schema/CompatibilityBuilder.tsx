import React from 'react';
import { Select, Typography } from 'antd';
import styled from 'styled-components';
import { SchemaAssertionCompatibility } from '../../../../../../../../../../types.generated';
import { compatibilityLevels } from './utils';

const Section = styled.div`
    margin-bottom: 20px;
`;

const StyledSelect = styled(Select)`
    width: 340px;
`;

const SelectOption = styled.div``;

const SelectDescription = styled(Typography.Paragraph)`
    && {
        word-wrap: break-word;
        white-space: break-spaces;
    }
`;

type Props = {
    selected?: SchemaAssertionCompatibility | null;
    onChange: (newCompatibility: SchemaAssertionCompatibility) => void;
    disabled?: boolean;
};

/**
 * Select the schema compatibility level.
 */
export const CompatibilityBuilder = ({ selected, onChange, disabled }: Props) => {
    return (
        <Section>
            <Typography.Title level={5}>Comparison Type</Typography.Title>
            <Typography.Paragraph type="secondary">
                Select the strictness of the schema assertion. This controls how the expected columns will be compared
                against the actual columns to determine a passing or failing state.
            </Typography.Paragraph>
            <StyledSelect
                value={selected}
                onChange={(s) => onChange(s as SchemaAssertionCompatibility)}
                disabled={disabled}
            >
                {compatibilityLevels.map((option) => {
                    return (
                        <Select.Option value={option.id} key={option.id}>
                            <SelectOption>
                                <Typography.Text>{option.name}</Typography.Text>
                                <SelectDescription type="secondary">{option.description}</SelectDescription>
                            </SelectOption>
                        </Select.Option>
                    );
                })}
            </StyledSelect>
        </Section>
    );
};
