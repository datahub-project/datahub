import React, { useEffect, useState } from 'react';
import TextArea from 'antd/lib/input/TextArea';
import Paragraph from 'antd/lib/typography/Paragraph';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import CheckOutlinedIcon from '@mui/icons-material/CheckOutlined';
import SectionActionButton from '../containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '../containers/profile/sidebar/SidebarSection';
import { AboutSection, AboutSectionText, EmptyValue } from '../SidebarStyledComponents';

type Props = {
    aboutText: string;
    isProfileOwner: boolean;
    onSaveAboutMe: (string: string) => void;
};

export const AboutSidebarSection = ({ aboutText, isProfileOwner, onSaveAboutMe }: Props) => {
    const [about, setAbout] = useState(aboutText);
    const [isAboutEditable, setIsAboutEditable] = useState(false);

    useEffect(() => {
        if (aboutText) {
            setAbout(aboutText);
        }
    }, [aboutText, setAbout]);

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
                            (
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
                            ) || <EmptyValue />
                        ) : (
                            <Paragraph ellipsis={{ rows: 2, expandable: true, symbol: 'Read more' }}>
                                {about || <EmptyValue />}
                            </Paragraph>
                        )}
                    </AboutSectionText>
                </AboutSection>
            }
            extra={
                <>
                    <SectionActionButton
                        button={isAboutEditable ? <CheckOutlinedIcon /> : <EditOutlinedIcon />}
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
