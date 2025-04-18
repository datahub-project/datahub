import React, { MouseEventHandler, useCallback } from 'react';
import { Select } from 'antd';
import styled from 'styled-components';
import { useActive, useCommands } from '@remirror/react';

const { Option } = Select;

const OPTIONS = [
    { tag: 'h1', label: 'Heading 1', value: 1 },
    { tag: 'h2', label: 'Heading 2', value: 2 },
    { tag: 'h3', label: 'Heading 3', value: 3 },
    { tag: 'h4', label: 'Heading 4', value: 4 },
    { tag: 'h5', label: 'Heading 5', value: 5 },
    { tag: 'p', label: 'Normal', value: 0 },
];

/* To mitigate overrides of the Select's width when using it in modals */
const Wrapper = styled.div`
    display: inline-block;
    width: 120px;
`;

const StyledSelect = styled(Select)`
    font-weight: 500;
    width: 100%;
`;

export const HeadingMenu = () => {
    const { toggleHeading } = useCommands();
    const active = useActive(true);

    const activeHeading = OPTIONS.map(({ value }) => value).filter((level) => active.heading({ level }))?.[0] || 0;

    const handleMouseDown: MouseEventHandler<HTMLDivElement> = useCallback((e) => {
        e.preventDefault();
    }, []);

    return (
        <Wrapper>
            <StyledSelect
                defaultValue={0}
                bordered={false}
                dropdownMatchSelectWidth={false}
                value={`${activeHeading}`}
                optionLabelProp="label"
                onChange={(value) => {
                    const level = +`${value}`;
                    if (level) {
                        toggleHeading({ level });
                    } else {
                        toggleHeading();
                    }
                }}
                onMouseDown={handleMouseDown}
            >
                {OPTIONS.map((option) => {
                    return (
                        <Option key={option.value} label={option.label} value={`${option.value}`}>
                            {React.createElement(option.tag, null, option.label)}
                        </Option>
                    );
                })}
            </StyledSelect>
        </Wrapper>
    );
};
