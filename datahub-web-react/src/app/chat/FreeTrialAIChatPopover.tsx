import { CalloutCard, CalloutPosition, Icon, Text } from '@components';
import React, { useMemo } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { FREE_TRIAL } from '@app/onboarding/configV2/FreeTrialConfig';
import { useFreeTrialPopoverVisibility } from '@app/sharedV2/freeTrial';
import { PageRoutes } from '@conf/Global';

const ContentText = styled(Text)`
    color: inherit;
    line-height: inherit;
`;

export type AIChatPopoverVariant = 'welcome' | 'completion';

interface PopoverConfig {
    stepIds: string[];
    icon: React.ReactNode;
    title: string;
    content: string;
    position: CalloutPosition;
    primaryButtonText: string;
    navigateToHome: boolean;
}

const POPOVER_CONFIGS: Record<AIChatPopoverVariant, PopoverConfig> = {
    welcome: {
        stepIds: [FREE_TRIAL.AI_CHAT_POPOVER_ID],
        icon: <Icon icon="Sparkle" source="phosphor" color="violet" size="xl" weight="fill" />,
        title: 'Ask DataHub',
        content: 'Chat interface for asking questions about your data and metadata.',
        position: 'inline',
        primaryButtonText: 'Close',
        navigateToHome: false,
    },
    completion: {
        stepIds: [FREE_TRIAL.AI_CHAT_COMPLETION_POPOVER_ID],
        icon: <Icon icon="Sparkle" source="phosphor" color="violet" size="xl" weight="fill" />,
        title: "You've Seen Ask DataHub",
        content: 'Continue learning about the platform and our connections to your Sources.',
        position: 'fixed-top-right',
        primaryButtonText: 'Go to Home',
        navigateToHome: true,
    },
};

interface Props {
    /** Which variant of the popover to show */
    variant: AIChatPopoverVariant;
}

/**
 * Popover component shown to free trial users on the AI Chat page.
 * - 'welcome' variant: Shown on empty state, introduces the chat feature
 * - 'completion' variant: Shown after user receives a response, encourages next steps
 */
export default function FreeTrialAIChatPopover({ variant }: Props) {
    const history = useHistory();
    const config = POPOVER_CONFIGS[variant];

    // Memoize stepIds array to prevent infinite loop in useEffect
    const stepIds = useMemo(() => config.stepIds, [config.stepIds]);

    const { isVisible, setIsVisible } = useFreeTrialPopoverVisibility({ stepIds });

    const handlePrimaryClick = () => {
        // TODO: Call API to mark step as dismissed/complete
        setIsVisible(false);
        if (config.navigateToHome) {
            history.push(PageRoutes.ROOT);
        }
    };

    if (!isVisible) {
        return null;
    }

    return (
        <CalloutCard
            icon={config.icon}
            title={config.title}
            position={config.position}
            primaryButtonText={config.primaryButtonText}
            onPrimaryClick={handlePrimaryClick}
        >
            <ContentText>{config.content}</ContentText>
        </CalloutCard>
    );
}
