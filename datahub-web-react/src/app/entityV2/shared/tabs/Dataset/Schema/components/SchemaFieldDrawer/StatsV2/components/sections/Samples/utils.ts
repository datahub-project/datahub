export function cleanSample(sample: string, maxLines: number | undefined = undefined) {
    const lines = sample.split('\n');

    // truncate the first empty lines
    const indexOfTheFirstNotEmptyLine = lines.findIndex((line) => /[^\s]/.test(line));
    const truncatedLines = indexOfTheFirstNotEmptyLine === -1 ? lines : lines.slice(indexOfTheFirstNotEmptyLine);

    return truncatedLines.slice(0, maxLines).join('\n');
}
