import React, { useState } from 'react';
import { Button, Select, message } from 'antd';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { useListRecommendationsQuery } from '../../../graphql/recommendations.generated';
import { DataPlatform, ScenarioType } from '../../../types.generated';
import { useUserContext } from '../../context/useUserContext';
import { capitalizeFirstLetter } from '../../shared/textUtil';
import { PLATFORMS_MODULE_ID } from '../content/tabs/discovery/sections/platform/useGetPlatforms';
import { ROLE_TO_PERSONA_TYPE } from '../shared/types';
import { useUpdateCorpUserPropertiesMutation } from '../../../graphql/user.generated';
import { useGetDataPlatforms } from '../content/tabs/discovery/sections/platform/useGetDataPlatforms';
import analytics, { EventType } from '../../analytics';
import PlatformIcon from '../../sharedV2/icons/PlatformIcon';

const Container = styled.div`
    flex: 1;
    // border-radius: 18px;
    padding-left: 52px;
    padding-right: 52px;
`;

const Content = styled.div`
    align-items: center;
    text-align: center;
    background-color: #ffffff;
    // border-radius: 18px;
    padding: 12px 20px 12px 20px;
    // border: 1.5px solid #efefef;
    min-height: 100%;
    display: flex;
    flex-direction: column;
    .ant-select-selection-item {
        align-items: center;
        gap: 4px;
        height: 42px !important;
    }
`;

const Title = styled.div`
    color: #1f202a;

    text-align: center;
    font-family: Mulish;
    font-size: 35px;
    font-style: normal;
    font-weight: 700;
    line-height: 44px; /* 125.714% */
    margin-top: 147px;
    margin-bottom: 3px;
`;

const Subtitle = styled.div`
    color: #1f202a;
    width: 268px;

    text-align: center;
    font-family: Mulish;
    font-size: 13px;
    font-style: normal;
    font-weight: 400;
    line-height: 21px; /* 161.538% */
    opacity: 0.6;
    margin-bottom: 62px;
`;

const DoneButton = styled(Button)`
    width: 352px;
    height: 45px;
    flex-shrink: 0;
    background-color: #3f54d1;
    color: #fff;
    margin-top: 12px;
`;

const SelectOption = styled.div`
    display: flex;
    gap: 8px;
    padding: 4px 0px;
    align-items: center;
`;

// const RoleCard = styled.div`
//     border: 1px solid #828da7;
//     border-radius: 7px;
//     height: 144px;
//     width: 112px;
//     display: flex;
//     flex-direction: column;
//     align-items: center;
// `;

// // this styled div is a 14x14 circle
// const RoleCardCircle = styled.div`
//     border-radius: 50%;
//     height: 14px;
//     width: 14px;
//     border: 1px solid #828da7;
//     // margin-top: 12px;
//     // margin-bottom: 12px;
//     background-color: #fff;
// `;

// const RoleCardComponent = ({
//     role,
//     prompt,
//     onSelect,
//     isSelected,
//     emoji,
// }: {
//     role: string;
//     prompt: string;
//     onSelect: (role: string) => void;
//     isSelected: boolean;
//     emoji: string;
// }) => {
//     return (
//         <RoleCard onClick={() => onSelect(role)}>
//             <div>
//                 <RoleCardCircle />
//             </div>
//             <div>{emoji}</div>
//             <div>{prompt}</div>
//         </RoleCard>
//     );
// };

// TODO: Make section ordering dynamic based on populated data.
export const IntroduceYourselfMainContent = () => {
    const { user, refetchUser } = useUserContext();
    const [selectedPersona, setSelectedPersona] = useState('');
    const [selectedPlatforms, setSelectedPlatforms] = useState();
    const [selectedTitle, setSelectedTitle] = useState();
    const defaultDataPlatforms = useGetDataPlatforms();
    const [updateCorpUserMutation, { loading }] = useUpdateCorpUserPropertiesMutation();

    const handlePersonaChange = (value: any) => {
        const personaType = ROLE_TO_PERSONA_TYPE[value];
        setSelectedPersona(personaType);
        setSelectedTitle(value);
    };

    const handlePlatformsChange = (value: any) => {
        setSelectedPlatforms(value);
    };

    const history = useHistory();
    const authenticatedUser = useUserContext();
    const currentUserUrn = authenticatedUser?.user?.urn || '';

    const { data } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: currentUserUrn as string,
                requestContext: {
                    scenario: ScenarioType.Home,
                },
                limit: 10,
            },
        },
        fetchPolicy: 'no-cache',
        skip: !currentUserUrn,
    });

    const platformsModule = data?.listRecommendations?.modules?.find(
        (module) => module.moduleId === PLATFORMS_MODULE_ID,
    );

    const getPlatformList = () => {
        if (platformsModule && platformsModule.content.length) {
            return platformsModule?.content
                ?.filter((content) => content.entity)
                .map((content) => ({
                    count: content.params?.contentParams?.count || 0,
                    platform: content.entity as DataPlatform,
                }));
        }
        return defaultDataPlatforms;
    };

    const platforms = getPlatformList();

    const onSubmitDetails = () => {
        updateCorpUserMutation({
            variables: {
                urn: user?.urn as string,
                input: {
                    personaUrn: selectedPersona,
                    platformUrns: selectedPlatforms,
                    title: selectedTitle,
                },
            },
        })
            .then(async () => {
                analytics.event({
                    type: EventType.IntroduceYourselfSubmitEvent,
                    role: selectedPersona,
                    platformUrns: selectedPlatforms || [],
                });
                await refetchUser();
                history.push('/');
            })
            .catch((err) => {
                console.log(err);
                message.error('Failed to save user details . :(');
            });
    };

    return (
        <Container>
            <Content>
                <Title>Before we begin</Title>
                <Subtitle>Tell us more about yourself, so we can personalize your experience</Subtitle>
                <Select
                    placeholder="Your Role"
                    size="large"
                    style={{ width: 352 }}
                    onChange={handlePersonaChange}
                    options={Object.keys(ROLE_TO_PERSONA_TYPE).map((key) => ({
                        value: key,
                        label: key,
                    }))}
                />
                <Select
                    placeholder="Tools you work with"
                    size="large"
                    style={{ width: 352, marginTop: 12 }}
                    onChange={handlePlatformsChange}
                    options={platforms.map((platform) => ({
                        value: platform.platform.urn,
                        label: (
                            <SelectOption>
                                <PlatformIcon platform={platform.platform} size={17} />{' '}
                                {capitalizeFirstLetter(platform.platform.name)}
                            </SelectOption>
                        ),
                    }))}
                    mode="multiple"
                />
                <DoneButton type="primary" size="large" onClick={onSubmitDetails} loading={loading}>
                    Done
                </DoneButton>
            </Content>
        </Container>
    );
};
