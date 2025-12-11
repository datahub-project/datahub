/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

/*
	Docs related utils 
*/

/**
 * Copies the given text to the clipboard.
 * @param {string} text - The text to be copied to the clipboard.
 * @returns {Promise<void>} A promise that resolves when the text is copied.
 */
export const copyToClipboard = (text: string) => {
    return navigator.clipboard
        .writeText(text)
        .then(() => console.log(`${text} copied to clipboard`))
        .catch();
};
