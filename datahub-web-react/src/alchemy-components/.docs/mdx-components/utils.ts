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
