import { colors } from '@components';
import { X } from '@phosphor-icons/react';
import styled from 'styled-components';

export const ToastContainer = styled.div<{ $sidebarWidth: number }>`
    display: inline-flex;
    flex-direction: column;
    align-items: flex-start;
    position: fixed;
    bottom: 18px;
    left: calc(${(props) => props.$sidebarWidth}px + 10px);
    width: 452px;
    max-width: calc(100vw - ${(props) => props.$sidebarWidth}px - 47px);
    max-height: calc(100vh - 48px);
    padding: 0;
    border-radius: 12px;
    background: linear-gradient(180deg, #f9fafc 0%, #f1f3fd 100%);
    box-shadow: 0 4px 28px 0 rgba(9, 1, 61, 0.14);
    z-index: 1000;
    transition:
        left 250ms ease-in-out,
        max-width 250ms ease-in-out;
    animation: slideUpScale 500ms cubic-bezier(0.34, 1.56, 0.64, 1);

    @keyframes slideUpScale {
        from {
            opacity: 0;
            transform: translateY(24px) scale(0.95);
        }
        to {
            opacity: 1;
            transform: translateY(0) scale(1);
        }
    }
`;

export const Header = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
    padding: 16px 16px 12px 16px;
    border-bottom: 1px solid ${colors.gray[200]};
    background: white;
    border-radius: 12px 12px 0 0;
`;

export const CloseButton = styled.button`
    background: none;
    border: none;
    color: ${colors.gray[400]};
    cursor: pointer;
    padding: 4px;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: color 0.2s;

    &:hover {
        color: ${colors.gray[600]};
    }
`;

export const StyledCloseIcon = styled(X)`
    font-size: 16px;
`;

export const Content = styled.div`
    width: 100%;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding: 16px 16px 0 16px;
    flex: 1;
    min-height: 0;
`;

export const HeroSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

export const ImageSection = styled.div``;

export const Image = styled.img`
    width: 100%;
    height: auto;
`;

export const SectionHeaderContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
    width: 100%;
`;

export const SectionHeaderLine = styled.div`
    flex: 1;
    height: 1px;
    background: ${colors.gray[200]};
`;

export const FeaturesSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

export const FeatureList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

export const FeatureItem = styled.div<{ $hasIcon: boolean }>`
    display: flex;
    align-items: flex-start;
    gap: ${(props) => (props.$hasIcon ? '10px' : '0')};
`;

export const FeatureIconWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    min-width: 20px;
    min-height: 20px;
    color: ${colors.gray[500]};
    flex-shrink: 0;
    margin-top: 2px;
`;

export const FeatureContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
    flex: 1;
`;

export const CTAContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: flex-end;
    align-items: center;
    gap: 8px;
    padding: 16px;
    border-top: 1px solid ${colors.gray[200]};
    width: calc(100% + 32px);
    background: white;
    border-radius: 0 0 12px 12px;
    margin: 0 -16px;
`;
