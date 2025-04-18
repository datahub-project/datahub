import { InfoCircleOutlined } from '@ant-design/icons';
import { Checkbox } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { RadioWrapper } from '@app/automations/fields/ApplyTypeSelector';
import type { ComponentBaseProps } from '@app/automations/types';
import { Tooltip } from '@src/alchemy-components';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const StyledLabel = styled.label`
    label {
        margin-right: 8px;
    }
`;

export type PropagationOptionsStateType = {
    includeDownstreams: true;
    includeSiblings: true;
};

export const PropagationOptions = ({ state, passStateToParent }: ComponentBaseProps) => {
    const handleRadioChange = (event) => {
        passStateToParent({
            [event.target.id]: event.target.checked,
        });
    };

    return (
        <Wrapper>
            <RadioWrapper>
                <StyledLabel htmlFor="includeDownstreams" aria-checked={state.includeDownstreams}>
                    <Checkbox
                        id="includeDownstreams"
                        value="includeDownstreams"
                        name="includeDownstreams"
                        checked={state.includeDownstreams}
                        onChange={handleRadioChange}
                    />
                    Propagate to downstreams
                </StyledLabel>
                <Tooltip showArrow={false} placement="right" title="Propagate metadata to downstream assets">
                    <InfoCircleOutlined />
                </Tooltip>
            </RadioWrapper>
            <RadioWrapper>
                <StyledLabel htmlFor="includeSiblings" aria-checked={state.includeSiblings}>
                    <Checkbox
                        id="includeSiblings"
                        value="includeSiblings"
                        name="includeSiblings"
                        checked={state.includeSiblings}
                        onChange={handleRadioChange}
                    />
                    Propagate to siblings
                </StyledLabel>
                <Tooltip showArrow={false} placement="right" title="Propagate metadata to sibling assets">
                    <InfoCircleOutlined />
                </Tooltip>
            </RadioWrapper>
        </Wrapper>
    );
};
