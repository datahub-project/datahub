/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export function cleanSample(sample: string, maxLines: number | undefined = undefined) {
    const lines = sample.split('\n');

    // truncate the first empty lines
    const indexOfTheFirstNotEmptyLine = lines.findIndex((line) => /[^\s]/.test(line));
    const truncatedLines = indexOfTheFirstNotEmptyLine === -1 ? lines : lines.slice(indexOfTheFirstNotEmptyLine);

    return truncatedLines.slice(0, maxLines).join('\n');
}
