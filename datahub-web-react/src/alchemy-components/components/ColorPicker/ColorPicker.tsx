import React, { useState, useEffect, useCallback } from 'react';
import styled from 'styled-components';
import { CirclePicker, ColorResult } from 'react-color';
import { Input, colors } from '@components';

// Constants
const DEFAULT_COLORS = [
    '#533FD1',
    '#29C7DC',
    '#FF6A24',
    '#248F5B',
    '#5F6685',
    '#80CBC4',
    '#09739A',
    '#90CAF9',
    '#7A85CD',
    '#EEAE09',
    '#CF6D6D',
    '#D23939',
    '#BF4636',
    '#FBC02D',
    '#5D4037',
    '#1F7523',
    '#41652C',
];

const DEFAULT_COLOR = '#000000';
const HEX_REGEX = /^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$/;

// Styled Components
const ColorPickerContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    width: 100%;
`;

const ColorPreview = styled.div`
    width: 100%;
    height: 100px;
    border-radius: 8px 8px 0px 0px;
    margin-top: 24px;
    border: 1px solid ${colors.gray[200]};
`;

const PickerWrapper = styled.div`
    width: 100%;
    display: flex;
`;

const HexInputContainer = styled.div`
    width: 100%;
`;

// Utility Functions
const formatHexColor = (hex: string): string => {
    // Ensure the hex starts with #
    let formattedHex = hex.startsWith('#') ? hex : `#${hex}`;

    // Expand shorthand hex (e.g., #RGB to #RRGGBB)
    if (formattedHex.length === 4) {
        const [r, g, b] = formattedHex.slice(1);
        formattedHex = `#${r}${r}${g}${g}${b}${b}`;
    }

    return formattedHex;
};

// Component
interface ColorPickerProps {
    initialColor?: string;
    onChange: (color: string) => void;
    // Using the label prop in the component implementation
    label?: string;
}

const ColorPicker: React.FC<ColorPickerProps> = ({ initialColor = DEFAULT_COLOR, onChange, label }) => {
    const [color, setColor] = useState(initialColor);
    const [hexInput, setHexInput] = useState(initialColor);
    const [hexError, setHexError] = useState('');

    // Reset state when initial color changes
    useEffect(() => {
        setColor(initialColor);
        setHexInput(initialColor);
        setHexError('');
    }, [initialColor]);

    // Validate and update color
    const updateColor = useCallback(
        (newColor: string) => {
            const formattedColor = formatHexColor(newColor);

            if (HEX_REGEX.test(formattedColor)) {
                setColor(formattedColor);
                setHexInput(formattedColor);
                setHexError('');
                onChange(formattedColor);
                return true;
            }

            setHexError('Please enter a valid hex color code');
            return false;
        },
        [onChange],
    );

    // Handle color picker change
    const handleColorChange = useCallback(
        (colorResult: ColorResult) => {
            updateColor(colorResult.hex);
        },
        [updateColor],
    );

    // Handle hex input change
    const handleHexInputChange = useCallback(
        (valueOrFn: React.SetStateAction<string>) => {
            const value = typeof valueOrFn === 'function' ? valueOrFn(hexInput) : valueOrFn;

            setHexInput(value);
            updateColor(value);
        },
        [hexInput, updateColor],
    );

    // Handle hex input blur
    const handleHexBlur = useCallback(() => {
        updateColor(hexInput || initialColor);
    }, [hexInput, initialColor, updateColor]);

    return (
        <ColorPickerContainer>
            {label && <div>{label}</div>}
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

            <ColorPreview style={{ backgroundColor: color }} />

            <HexInputContainer>
                <Input
                    label=""
                    value={hexInput}
                    setValue={handleHexInputChange}
                    placeholder="#000000"
                    error={hexError}
                    isInvalid={!!hexError}
                    onBlur={handleHexBlur}
                />
            </HexInputContainer>
        </ColorPickerContainer>
    );
};

export { ColorPicker };
export default ColorPicker;
