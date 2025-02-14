import colors from '@src/alchemy-components/theme/foundations/colors';
import styled from 'styled-components';

export const Title = styled.div`
    color: ${colors.gray[1700]};
    font-size: 14px;
    font-weight: 400;
`;

export const TitleSuffix = styled.div`
    margin-left: 4px;
`;

export const SectionsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

export const Section = styled.div``;

export const SectionHeader = styled.div`
    display: flex;
    align-items: flex-start;
`;

export const SectionTitle = styled.div`
    font-weight: 700;
    font-size: 12px;
    color: ${colors.gray[600]};
`;

export const Content = styled.div`
    margin-top: 4px;
    color: ${colors.gray[1700]};
    font-size: 14px;
`;

export const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;
