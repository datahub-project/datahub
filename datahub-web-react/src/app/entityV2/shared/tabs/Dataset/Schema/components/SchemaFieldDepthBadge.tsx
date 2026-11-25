import { Icon, Tooltip } from '@components';
import { Rows } from '@phosphor-icons/react/dist/csr/Rows';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const BadgeContainer = styled.span`
    position: relative;
    display: inline-flex;
    align-items: center;
    flex-shrink: 0;
`;

const DepthNumber = styled.span<{ $multipleDigits: boolean }>`
    position: absolute;
    top: -6px;
    right: -6px;
    display: flex;
    align-items: center;
    justify-content: center;
    min-width: ${(props) => (props.$multipleDigits ? '20px' : '13px')};
    height: ${(props) => (props.$multipleDigits ? '20px' : '13px')};
    border-radius: 50%;
    background: ${(props) => props.theme.colors.bgSurfaceBrand};
    color: ${(props) => props.theme.colors.textOnFillDefault};
    font-size: 10px;
    font-weight: 400;
`;

/** Shows how deeply a nested schema field sits, e.g. while search results flatten the tree. */
export default function SchemaFieldDepthBadge({ depth }: { depth: number }) {
    const { t } = useTranslation('entity.profile.schema');
    const level = depth + 1;

    return (
        <Tooltip title={t('expandIcon.levelsNested', { count: level })} placement="bottom">
            <BadgeContainer data-testid="schema-field-depth-badge">
                <Icon icon={Rows} size="lg" color="icon" />
                <DepthNumber $multipleDigits={level >= 10}>{level}</DepthNumber>
            </BadgeContainer>
        </Tooltip>
    );
}
