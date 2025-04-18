import { Button, Switch } from 'antd';
import React from 'react';
import styled from 'styled-components';
import useHelpLinkForm from './useHelpLinkForm';
import LinkInput, { getLinkWithoutPrefix } from './LinkInput';
import { InputLabel, InputWrapper, RequiredIcon, StyledInput } from './components';

const ContentWrapper = styled.div`
    display: flex;
    padding-left: 24px;
    margin-top: 24px;
    flex: 1;
`;

const SettingText = styled.div`
    font-size: 16px;
    font-weight: 600;
`;

const DescriptionText = styled.div`
    color: #373d44;
    font-size: 14px;
`;

const StyledSwitch = styled(Switch)`
    margin-right: 16px;
    margin-top: 2px;
`;

const ButtonInputsWrapper = styled.div`
    margin-top: 24px;
`;

export default function HelpLinkForm() {
    const { isEnabled, toggleHelpLink, label, setLabel, link, saveHelpLink } = useHelpLinkForm();
    const linkWithoutPrefix = getLinkWithoutPrefix(link);

    return (
        <ContentWrapper>
            <StyledSwitch checked={isEnabled} onChange={toggleHelpLink} />
            <div>
                <SettingText>Enable custom help link</SettingText>
                <DescriptionText>
                    Enable a universal help link in the help menu to direct your users to any URL such as an internet
                    page or Slack channel.
                </DescriptionText>
                {isEnabled && (
                    <ButtonInputsWrapper>
                        <InputLabel>
                            Button Label<RequiredIcon>*</RequiredIcon>
                        </InputLabel>
                        <InputWrapper>
                            <StyledInput value={label} onChange={(e) => setLabel(e.target.value)} />
                        </InputWrapper>
                        <LinkInput />
                        <Button type="primary" onClick={saveHelpLink} disabled={!label || !linkWithoutPrefix}>
                            Save
                        </Button>
                    </ButtonInputsWrapper>
                )}
            </div>
        </ContentWrapper>
    );
}
