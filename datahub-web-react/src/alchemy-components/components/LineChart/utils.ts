export function roundToEven(value: number) {
    const rounded = Math.ceil(value);
    return rounded % 2 === 0 ? rounded : rounded + 1;
}
