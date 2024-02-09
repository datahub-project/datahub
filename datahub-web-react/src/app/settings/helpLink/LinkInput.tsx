import { Select } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { InputLabel, InputWrapper, RequiredIcon, StyledInput } from './components';
import useHelpLinkForm from './useHelpLinkForm';

const InputWithSelect = styled(StyledInput)`
    &&& {
        border-radius: 0 5px 5px 0;
        border-left: 0px;
    }
`;

const StyledSelect = styled(Select)`
    height: min-content;
    &&& {
        .ant-select-selector {
            border-radius: 5px 0 0 5px;
        }
    }
`;

const OPTIONS = [
    {
        value: 'https://',
        label: 'https://',
    },
    {
        value: 'mailto:',
        label: 'mailto:',
    },
    {
        value: 'http://',
        label: 'http://',
    },
];

export function getLinkPrefixRegEx() {
    return new RegExp(OPTIONS.map((entry) => entry.value).join('|'), 'gi');
}

export function getLinkPrefix(link: string) {
    const linkPrefixRegEx = getLinkPrefixRegEx();
    return link.match(linkPrefixRegEx)?.[0];
}

export function getLinkWithoutPrefix(link: string) {
    const linkPrefixRegEx = getLinkPrefixRegEx();
    return link.replace(linkPrefixRegEx, '');
}

export default function LinkInput() {
    const { link, setLink } = useHelpLinkForm();
    const [linkPrefix, setLinkPrefix] = useState(getLinkPrefix(link) || OPTIONS[0].value);
    const linkWithoutPrefix = getLinkWithoutPrefix(link);

    function updateLinkPrefix(prefix: string) {
        setLinkPrefix(prefix);
        setLink(`${prefix}${linkWithoutPrefix}`);
    }

    function updateLinkContent(content: string) {
        const contentWithoutPrefix = getLinkWithoutPrefix(content);
        setLink(`${linkPrefix}${contentWithoutPrefix}`);
    }

    return (
        <>
            <InputLabel>
                Link<RequiredIcon>*</RequiredIcon>
            </InputLabel>
            <InputWrapper>
                <StyledSelect
                    value={linkPrefix}
                    options={OPTIONS}
                    onChange={(value) => updateLinkPrefix(value as string)}
                />
                <InputWithSelect
                    value={linkWithoutPrefix}
                    onChange={(e) => updateLinkContent(e.target.value)}
                    style={{ display: 'inline-block' }}
                />
            </InputWrapper>
        </>
    );
}
