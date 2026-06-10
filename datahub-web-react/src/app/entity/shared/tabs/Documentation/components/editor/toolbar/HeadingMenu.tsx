import { useActive, useCommands } from '@remirror/react';
import { Select } from 'antd';
import React, { MouseEventHandler, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const { Option } = Select;

function useHeadingOptions() {
    const { t } = useTranslation('entity.profile.editor');
    return [
        { tag: 'h1', label: t('heading.heading1'), value: 1 },
        { tag: 'h2', label: t('heading.heading2'), value: 2 },
        { tag: 'h3', label: t('heading.heading3'), value: 3 },
        { tag: 'h4', label: t('heading.heading4'), value: 4 },
        { tag: 'h5', label: t('heading.heading5'), value: 5 },
        { tag: 'p', label: t('heading.normal'), value: 0 },
    ];
}

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
    const options = useHeadingOptions();

    const activeHeading = options.map(({ value }) => value).filter((level) => active.heading({ level }))?.[0] || 0;

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
                {options.map((option) => {
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
