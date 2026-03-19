import { Button, TextArea } from '@components';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { AboutSection, AboutSectionText, EmptyValue } from '@app/entityV2/shared/SidebarStyledComponents';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';

const VISIBLE_LINES = 2;

const ClampedText = styled.div<{ $expanded: boolean }>`
    ${(props) =>
        !props.$expanded &&
        `
        display: -webkit-box;
        -webkit-line-clamp: ${VISIBLE_LINES};
        line-clamp: ${VISIBLE_LINES};
        -webkit-box-orient: vertical;
        overflow: hidden;
    `}
`;

type Props = {
    aboutText: string;
    isProfileOwner: boolean;
    onSaveAboutMe: (string: string) => void;
};

export const AboutSidebarSection = ({ aboutText, isProfileOwner, onSaveAboutMe }: Props) => {
    const [about, setAbout] = useState(aboutText);
    const [isAboutEditable, setIsAboutEditable] = useState(false);
    const [isExpanded, setIsExpanded] = useState(false);
    const [isClamped, setIsClamped] = useState(false);
    const textRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (aboutText) {
            setAbout(aboutText);
        }
    }, [aboutText, setAbout]);

    useEffect(() => {
        const el = textRef.current;
        if (el) {
            setIsClamped(el.scrollHeight > el.clientHeight);
        }
    }, [about, isAboutEditable]);

    const onSave = (value: string) => {
        setIsAboutEditable(false);
        onSaveAboutMe(value);
    };

    return (
        <SidebarSection
            title="About"
            content={
                <AboutSection>
                    <AboutSectionText>
                        {isProfileOwner && isAboutEditable ? (
                            <TextArea
                                value={about}
                                onChange={(e) => setAbout(e.target.value)}
                                onBlur={(event) => {
                                    if (aboutText !== event.target.value) {
                                        onSave(event.target.value);
                                    }
                                    setIsAboutEditable(false);
                                }}
                            />
                        ) : (
                            <>
                                <ClampedText ref={textRef} $expanded={isExpanded}>
                                    {about || <EmptyValue />}
                                </ClampedText>
                                {isClamped && (
                                    <Button
                                        variant="link"
                                        color="gray"
                                        size="sm"
                                        onClick={() => setIsExpanded((prev) => !prev)}
                                    >
                                        {isExpanded ? 'Show less' : 'Read more'}
                                    </Button>
                                )}
                            </>
                        )}
                    </AboutSectionText>
                </AboutSection>
            }
            extra={
                <>
                    <SectionActionButton
                        icon={isAboutEditable ? Check : PencilSimple}
                        onClick={(event) => {
                            if (isProfileOwner) {
                                if (aboutText !== about) {
                                    onSave(about);
                                }
                                setIsAboutEditable(!isAboutEditable);
                            }
                            event.stopPropagation();
                        }}
                        actionPrivilege={isProfileOwner}
                    />
                </>
            }
        />
    );
};
