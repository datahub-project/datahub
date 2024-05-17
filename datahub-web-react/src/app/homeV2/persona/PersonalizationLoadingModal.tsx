import React, { useCallback, useContext, useEffect } from 'react';
import { Modal, Progress } from 'antd';
import styled from 'styled-components';
import OnboardingContext from '../../onboarding/OnboardingContext';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';

const ModalStyle = styled(Modal)`
    .ant-modal-content {
        border-radius: 8px;
        border: 1px solid #c1c4d0;
        border-bottom: unset;
    }

    .ant-modal-body {
        padding: 0;
    }
`;

const ProgressStyle = styled(Progress)`
    .ant-progress-inner {
        border-radius: 12px;
        vertical-align: bottom;
    }

    .ant-progress-bg {
        border-radius: 12px;
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        height: 4px !important;
    }
`;

const ModalContent = styled.span`
    font-weight: 700;
    display: flex;
    justify-content: center;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    padding: 25px 0 0;
    font-size: 16px;
`;

const modalMaskStyle = {
    backgroundColor: 'rgba(0, 0, 0, 0.05)',
};

export default function PersonalizationLoadingModal() {
    const { isUserInitializing, setIsUserInitializing } = useContext(OnboardingContext);
    const [percent, setPercent] = React.useState(0);

    const increaseProgress = useCallback(
        (intervalId: NodeJS.Timeout) => {
            const newPercent = Math.min(percent + 20, 100);
            setPercent(newPercent);
            if (newPercent === 100) {
                clearInterval(intervalId);
                setIsUserInitializing(false);
            }
        },
        [percent, setIsUserInitializing],
    );

    useEffect(() => {
        const intervalId = setInterval(() => increaseProgress(intervalId), 1000);
        return () => clearInterval(intervalId);
    }, [increaseProgress]);

    return (
        <ModalStyle
            open={isUserInitializing}
            footer={null}
            title={null}
            wrapClassName="popup-modal"
            centered
            closable={false}
            maskStyle={modalMaskStyle}
        >
            <ModalContent>{`Hang tight! We're getting everything ready for you.`}</ModalContent>
            <ProgressStyle percent={percent} showInfo={false} />
        </ModalStyle>
    );
}
