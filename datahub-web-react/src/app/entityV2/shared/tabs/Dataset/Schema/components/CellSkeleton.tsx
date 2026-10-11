import styled from 'styled-components';

/**
 * Shimmer placeholder for a table cell whose content is still loading (e.g. Phase 2 of the
 * two-phase schema load). One shared implementation on the dedicated skeleton theme tokens —
 * used by SchemaTable's metadata columns and the structured-property columns alike.
 */
const CellSkeleton = styled.div<{ $width?: number }>`
    width: ${(props) => props.$width ?? 80}px;
    height: 20px;
    border-radius: 4px;
    background: linear-gradient(
        90deg,
        ${(props) => props.theme.colors.bgSkeleton} 25%,
        ${(props) => props.theme.colors.bgSkeletonShimmer} 37%,
        ${(props) => props.theme.colors.bgSkeleton} 63%
    );
    background-size: 400% 100%;
    animation: cell-skeleton-shimmer 1.4s ease infinite;
    @keyframes cell-skeleton-shimmer {
        0% {
            background-position: 100% 50%;
        }
        100% {
            background-position: 0 50%;
        }
    }
`;

export default CellSkeleton;
