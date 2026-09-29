import { Input } from '@components';
import React, { useCallback, useEffect, useState } from 'react';
import { CirclePicker, ColorResult } from 'react-color';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { formLabelTextStyles } from '@components/components/commonStyles';
import { spacing } from '@components/theme';

const HEX_REGEX = /^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$/;
const FULL_HEX_REGEX = /^#[A-Fa-f0-9]{6}$/;

const ColorPickerContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    width: 100%;
`;

// Mirrors the Label styling shared by alchemy <Input>, <TextArea>, and <Switch>
// so a ColorPicker label reads identically next to those other form labels.
const Label = styled.div(({ theme }) => ({
    ...formLabelTextStyles,
    color: theme.colors.text,
    marginBottom: spacing.xsm,
    textAlign: 'left' as const,
}));

const ColorPreview = styled.div<{ $hasDotsAbove: boolean }>`
    width: 100%;
    height: 100px;
    border-radius: 8px 8px 0px 0px;
    margin-top: ${(props) => (props.$hasDotsAbove ? '24px' : '0')};
    border: 1px solid ${(props) => props.theme.colors.border};
`;

const PickerWrapper = styled.div`
    width: 100%;
    display: flex;
`;

const HexInputContainer = styled.div`
    width: 100%;
`;

/**
 * Normalizes a hex string: adds a leading # and expands #RGB → #RRGGBB.
 * Only call this when committing a finished value (blur / swatch), never while typing —
 * expanding on every keystroke traps users who delete through #RGB (e.g. #fff → #ffffff).
 */
const formatHexColor = (hex: string): string => {
    let formattedHex = hex.startsWith('#') ? hex : `#${hex}`;

    if (formattedHex.length === 4) {
        const [r, g, b] = formattedHex.slice(1);
        formattedHex = `#${r}${r}${g}${g}${b}${b}`;
    }

    return formattedHex;
};

type Props = {
    initialColor?: string;
    onChange: (color: string) => void;
    label?: string;
    /** Preset color dots (CirclePicker). On by default; turn off for hex-only flows. */
    showDots?: boolean;
};

/**
 * Hex color input with optional preset swatches. Incomplete values stay editable;
 * empty clears via onChange('').
 */
export function ColorPicker({ initialColor, onChange, label, showDots = true }: Props): React.ReactElement {
    const { t } = useTranslation('alchemy');
    const theme = useTheme();

    const defaultColor = initialColor || theme.colors.colorPickerDefault;

    const [color, setColor] = useState(defaultColor);
    const [hexInput, setHexInput] = useState(defaultColor);
    const [hexError, setHexError] = useState('');

    const DEFAULT_COLORS = [
        theme.colors.chartsBrandHigh,
        theme.colors.chartsBlueMedium,
        theme.colors.colorPickerOrange,
        theme.colors.iconSuccess,
        theme.colors.textSecondary,
        theme.colors.chartsSeafoamLow,
        theme.colors.textInformation,
        theme.colors.colorPickerBlue,
        theme.colors.colorPickerCobalt,
        theme.colors.iconWarning,
        theme.colors.chartsWineMedium,
        theme.colors.textError,
        theme.colors.colorPickerTangerine,
        theme.colors.tagsTrueYellowIcon,
        theme.colors.colorPickerBrown,
        theme.colors.colorPickerDarkGreen,
        theme.colors.colorPickerOlive,
    ];

    // Reset state when initial color changes from outside (e.g. reset to default).
    useEffect(() => {
        setColor(defaultColor);
        setHexInput(defaultColor);
        setHexError('');
    }, [defaultColor, initialColor]);

    const commitColor = useCallback(
        (raw: string): boolean => {
            const formattedColor = formatHexColor(raw.trim());

            if (!HEX_REGEX.test(formattedColor)) {
                setHexError(t('colorPicker.invalidHex.error'));
                return false;
            }

            setColor(formattedColor);
            setHexInput(formattedColor);
            setHexError('');
            onChange(formattedColor);
            return true;
        },
        [onChange, t],
    );

    const handleColorChange = useCallback(
        (colorResult: ColorResult) => {
            commitColor(colorResult.hex);
        },
        [commitColor],
    );

    /**
     * While typing: keep the raw string, never expand shorthand.
     * Only live-commit complete 6-digit hex so partial values (#f, #fff) stay editable.
     * Empty notifies the parent so Save can clear the stored brand color.
     */
    const handleHexInputChange = useCallback(
        (valueOrFn: React.SetStateAction<string>) => {
            const value = typeof valueOrFn === 'function' ? valueOrFn(hexInput) : valueOrFn;
            setHexInput(value);

            const trimmed = value.trim();
            if (trimmed === '') {
                setHexError('');
                onChange('');
                return;
            }

            if (FULL_HEX_REGEX.test(trimmed)) {
                const formatted = formatHexColor(trimmed);
                setColor(formatted);
                setHexError('');
                onChange(formatted);
                return;
            }

            // Incomplete or invalid while typing — show error only for bad characters / over-length.
            const withoutHash = trimmed.startsWith('#') ? trimmed.slice(1) : trimmed;
            const looksInvalid =
                withoutHash.length > 6 || (withoutHash.length > 0 && !/^[A-Fa-f0-9]+$/.test(withoutHash));
            setHexError(looksInvalid ? t('colorPicker.invalidHex.error') : '');
        },
        [hexInput, onChange, t],
    );

    const handleHexBlur = useCallback(() => {
        const trimmed = hexInput.trim();
        if (trimmed === '') {
            setHexError('');
            onChange('');
            return;
        }
        commitColor(trimmed);
    }, [commitColor, hexInput, onChange]);

    return (
        <ColorPickerContainer>
            {label && <Label aria-label={label}>{label}</Label>}
            {showDots && (
                <PickerWrapper>
                    <CirclePicker
                        colors={DEFAULT_COLORS}
                        color={color}
                        onChange={handleColorChange}
                        width="100%"
                        circleSize={32}
                        circleSpacing={8}
                    />
                </PickerWrapper>
            )}

            <ColorPreview $hasDotsAbove={showDots} style={{ backgroundColor: color }} />

            <HexInputContainer>
                <Input
                    label=""
                    value={hexInput}
                    setValue={handleHexInputChange}
                    placeholder={defaultColor}
                    error={hexError}
                    isInvalid={!!hexError}
                    onBlur={handleHexBlur}
                />
            </HexInputContainer>
        </ColorPickerContainer>
    );
}
