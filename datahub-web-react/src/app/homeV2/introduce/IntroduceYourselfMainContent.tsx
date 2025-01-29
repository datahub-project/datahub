import { Tooltip } from '@components';
import AccountCircleOutlinedIcon from '@mui/icons-material/AccountCircleOutlined';
import CheckIcon from '@mui/icons-material/Check';
import KeyboardArrowDownOutlinedIcon from '@mui/icons-material/KeyboardArrowDownOutlined';
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined';
import colors from '@src/alchemy-components/theme/foundations/colors';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useListGlobalViewsQuery } from '@src/graphql/view.generated';
import { Button, Select, message } from 'antd';
import { orderBy } from 'lodash';
import React, { useContext, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { useListRecommendationsQuery } from '../../../graphql/recommendations.generated';
import {
    useUpdateCorpUserPropertiesMutation,
    useUpdateCorpUserViewsSettingsMutation,
} from '../../../graphql/user.generated';
import { DataHubViewType, DataPlatform, EntityType, ScenarioType } from '../../../types.generated';
import analytics, { EventType } from '../../analytics';
import { useUserContext } from '../../context/useUserContext';
import OnboardingContext from '../../onboarding/OnboardingContext';
import Loading from '../../shared/Loading';
import PlatformIcon from '../../sharedV2/icons/PlatformIcon';
import { useGetDataPlatforms } from '../content/tabs/discovery/sections/platform/useGetDataPlatforms';
import { PLATFORMS_MODULE_ID } from '../content/tabs/discovery/sections/platform/useGetPlatforms';
import { PERSONA_TYPE_TO_VIEW_URN, PersonaType, ROLE_TO_PERSONA_TYPE } from '../shared/types';

const Container = styled.div`
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 0 52px;
`;

const Content = styled.div`
    background-color: #ffffff;
    padding: 20px;

    .ant-select-selection-item {
        align-items: center;
        gap: 4px;
        height: 42px !important;
    }

    .ant-select-selection-overflow-item-rest {
        .ant-select-selection-item {
            background-color: #fff !important;
            border: none !important;
            padding: 0 0 0 5px !important;
            height: auto !important;
            font-size: 12px;
            line-height: 20px;
        }
    }
`;

const Title = styled.div`
    color: #374066;
    text-align: center;
    font: 700 35px Mulish;
    line-height: 44px;
    margin-bottom: 3px;
`;

const Subtitle = styled.div`
    color: #5f6685;
    width: 268px;
    text-align: center;
    font: 400 13px Mulish;
    line-height: 21px;
    margin-bottom: 28px;
`;

const DoneButton = styled(Button)`
    width: 290px;
    height: 45px;
    flex-shrink: 0;
    background-color: #3f54d1;
    color: #fff;
    margin-top: 12px;
`;

const PsuedoCheckBox = styled.div<{ checked?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    position: absolute;
    top: 0;
    left: 0;
    width: 12px;
    height: 12px;
    border-radius: 4px;
    border: 1px solid #cfd1da;
    background: #fff;
    color: #fff;

    ${(props) =>
        props.checked &&
        `
        background: #533fd1;
        border: none;
    `}

    & svg {
        width: 10px;
        height: 10px;
    }
`;

const SelectWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: flex-start;
    position: relative;

    & + & {
        margin-top: 12px;
    }

    & svg {
        position: absolute;
        left: 10px;
        z-index: 99;
        fill: #a9adbd;
    }

    .ant-select-arrow {
        & svg {
            position: relative;
            margin-right: 5px;
        }
    }

    .ant-select-selection-item,
    .ant-select-selection-placeholder,
    .ant-select-selection-search {
        padding-left: 30px !important;
    }

    .ant-select-selection-overflow {
        padding-left: 30px !important;

        .ant-select-selection-search {
            padding-left: 0px !important;
        }
    }
`;

const SelectGrid = styled.div`
    .rc-virtual-list-holder-inner {
        max-width: 290px;
        display: grid !important;
        grid-template-columns: repeat(4, 1fr);
        grid-gap: 10px;
        padding: 10px;
    }

    .ant-select-item {
        padding: 0;
        margin: 0;

        &:hover,
        &:focus,
        &:active {
            background-color: #fff !important;
        }
    }

    .ant-select-item-option-active:not(.ant-select-item-option-disabled) {
        background-color: #fff !important;
    }

    .ant-select-item-option-content {
        display: flex;
        justify-content: center;
        background-color: #fff !important;

        &:hover,
        &:focus,
        &:active {
            background-color: #fff !important;
        }
    }
`;

const SelectOption = styled.div`
    display: flex;
    position: relative;
    overflow: hidden;
    width: 40px;
    height: 40px;
`;

const SelectTag = styled.div`
    margin-right: 4px;
`;

const Footer = styled.div`
    margin-top: 16px;
    display: flex;
    justify-content: center;
    align-items: center;
`;

const SkipButton = styled.div`
    color: ${colors.gray[400]};
    font-weight: 700;
    :hover {
        cursor: pointer;
    }
`;

const DEFAULT_PERSONA = PersonaType.TECHNICAL_USER;

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
    const userContext = useUserContext();
    const { refetchUser, user } = userContext;
    const defaultDataPlatforms = useGetDataPlatforms();
    const [updateCorpUserMutation, { loading }] = useUpdateCorpUserPropertiesMutation();
    const [updateUserViewSettingMutation] = useUpdateCorpUserViewsSettingsMutation();
    const history = useHistory();
    const authenticatedUser = useUserContext();
    const currentUserUrn = authenticatedUser?.user?.urn || '';
    const entityRegistry = useEntityRegistry();

    // commented out for now, but may be brought back in the future
    // const [selectedPersona, setSelectedPersona] = useState<string>(PersonaType.TECHNICAL_USER);
    const selectedPersona = PersonaType.TECHNICAL_USER;
    const [selectedPlatforms, setSelectedPlatforms] = useState<string[]>([]);
    const [selectedTitle, setSelectedTitle] = useState('');

    const { loading: viewsLoading, data: globalViewsData } = useListGlobalViewsQuery({
        variables: {
            start: 0,
            count: 100,
        },
        fetchPolicy: 'no-cache',
    });

    const globalViews = globalViewsData?.listGlobalViews?.views || [];

    const { data, loading: reccosLoading } = useListRecommendationsQuery({
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

    const handleRoleChange = (value: string) => {
        setSelectedTitle(value);
    };

    const { setIsUserInitializing } = useContext(OnboardingContext);

    /**
     * Updates the User's Personal Default View via mutation.
     *
     * Then updates the User Context state to contain the new default.
     */
    const setUserDefault = (viewUrn: string | null) => {
        return updateUserViewSettingMutation({
            variables: {
                input: {
                    defaultView: viewUrn,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    userContext.updateState({
                        ...userContext.state,
                        views: {
                            ...userContext.state.views,
                            personalDefaultViewUrn: viewUrn,
                        },
                    });
                    userContext.updateLocalState({
                        ...userContext.localState,
                        selectedViewUrn: viewUrn,
                    });
                    analytics.event({
                        type: EventType.SetUserDefaultViewEvent,
                        urn: viewUrn,
                        viewType: (viewUrn && DataHubViewType.Global) || null,
                    });
                }
            })
            .catch((_) => {
                message.destroy();
                message.error({
                    content: `Failed to provision a default view. An unexpected error occurred.`,
                    duration: 3,
                });
            });
    };

    const onSubmitDetails = () => {
        setIsUserInitializing(true);

        // The default views for each persona needs to be created prior
        const personaDefaultView = globalViews.find((view) => view.urn === PERSONA_TYPE_TO_VIEW_URN[selectedPersona]);
        const { globalDefaultViewUrn } = userContext.state.views;

        if (personaDefaultView && !globalDefaultViewUrn) {
            setUserDefault(personaDefaultView.urn);
        }

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
                console.error(err);
                message.error('Failed to save user details. :(');
            });
    };

    const onSkip = () => {
        setIsUserInitializing(true);

        updateCorpUserMutation({
            variables: {
                urn: user?.urn as string,
                input: {
                    personaUrn: DEFAULT_PERSONA,
                    platformUrns: [],
                },
            },
        })
            .then(async () => {
                analytics.event({
                    type: EventType.IntroduceYourselfSkipEvent,
                });
                await refetchUser();
                history.push('/');
            })
            .catch((err) => {
                console.error(err);
                message.error('Failed to save user details. :(');
            });
    };

    const hasPersona = !!selectedPersona;
    // Possibly needed in the future
    // const hasPlatforms = selectedPlatforms.length > 0;
    // const canSubmit = hasPersona && hasPlatforms;

    const selectStyles = {
        width: 290,
        borderRadius: '8px',
        borderColor: '#5F6685',
        color: '#81879f',
    };

    // Sort Roles Alphabetically
    const sortedRoles = orderBy(Object.keys(ROLE_TO_PERSONA_TYPE), (role) => role);

    // Move 'Other' to the end of the list
    const updatedRoles = sortedRoles.filter((role) => role !== 'Other');
    updatedRoles.push('Other');

    // Get window height
    const windowHeight = window.innerHeight;
    const smallWindow = windowHeight <= 719;

    // Show loading state
    const isLoading = loading && reccosLoading && viewsLoading;
    if (isLoading) return <Loading />;

    return (
        <Container>
            <Content>
                <Title>Before we begin</Title>
                <Subtitle>Tell us more about yourself, so we can personalize your experience</Subtitle>
                <SelectWrapper>
                    <AccountCircleOutlinedIcon />
                    <Select
                        placeholder="Select your Role"
                        suffixIcon={<KeyboardArrowDownOutlinedIcon />}
                        data-testid="introduce-role-select"
                        size="large"
                        style={selectStyles}
                        onChange={handleRoleChange}
                        showSearch
                        autoFocus
                    >
                        {updatedRoles.map((role) => (
                            <Select.Option key={role} value={role} data-testid={`role-option-${role}`}>
                                {role}
                            </Select.Option>
                        ))}
                    </Select>
                </SelectWrapper>
                <SelectWrapper>
                    <SettingsOutlinedIcon />
                    <Select
                        placeholder="Optional - Select your Data Tools"
                        size="large"
                        style={selectStyles}
                        onChange={(value) => setSelectedPlatforms(value)}
                        options={platforms.map((platform) => {
                            const { urn } = platform.platform;
                            const isChecked = !!selectedPlatforms.includes(urn);

                            const displayName = entityRegistry.getDisplayName(
                                EntityType.DataPlatform,
                                platform.platform,
                            );
                            return {
                                value: platform.platform.urn,
                                label: (
                                    <SelectOption>
                                        <Tooltip title={displayName} placement="left" mouseEnterDelay={0.5}>
                                            <PsuedoCheckBox checked={isChecked}>
                                                {isChecked && <CheckIcon />}
                                            </PsuedoCheckBox>
                                            <PlatformIcon
                                                platform={platform.platform}
                                                size={24}
                                                styles={{ width: '40px', height: '40px' }}
                                            />
                                        </Tooltip>
                                    </SelectOption>
                                ),
                            };
                        })}
                        dropdownRender={(menu) => <SelectGrid>{menu}</SelectGrid>}
                        tagRender={(props: any) => {
                            const { value } = props;
                            const platform = platforms.find((p) => p.platform.urn === value);
                            return (
                                <SelectTag>
                                    <PlatformIcon platform={platform?.platform as DataPlatform} size={14} />
                                </SelectTag>
                            );
                        }}
                        mode="multiple"
                        maxTagCount={5}
                        maxTagPlaceholder={(values) => `${values.length}+`}
                        virtual={false}
                        listHeight={smallWindow ? 100 : 300}
                        placement="bottomLeft"
                        menuItemSelectedIcon
                    />
                </SelectWrapper>
                {/* Note: This is commented out for now, but may be brought back in the future. As of today, it causes more confusion than it helps */}
                {/* <PersonaSelector selectedPersona={selectedPersona} onSelect={setSelectedPersona} /> */}
                <DoneButton
                    type="primary"
                    size="large"
                    onClick={onSubmitDetails}
                    loading={loading}
                    disabled={!hasPersona}
                >
                    Get Started
                </DoneButton>
                <Footer>
                    <Tooltip placement="bottom" title="Continue to DataHub">
                        <SkipButton onClick={onSkip}>Skip</SkipButton>
                    </Tooltip>
                </Footer>
            </Content>
        </Container>
    );
};
