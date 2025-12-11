/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import { createGlobalStyle } from 'styled-components';

import '../src/fonts/Mulish-Regular.ttf';
import '../src/fonts/Mulish-Medium.ttf';
import '../src/fonts/Mulish-SemiBold.ttf';
import '../src/fonts/Mulish-Bold.ttf';

export const GlobalStyle = createGlobalStyle`
    @font-face {
		font-family: 'Mulish';
		font-style: normal;
		font-weight: 400;
		src: url('../src/fonts/Mulish-Regular.ttf) format('truetype');
    }
	@font-face {
		font-family: 'Mulish';
		font-style: normal;
		font-weight: 500;
		src: url('../src/fonts/Mulish-Medium.ttf) format('truetype');
	}
	@font-face {
		font-family: 'Mulish';
		font-style: normal;
		font-weight: 600;
		src: url('../src/fonts/Mulish-SemiBold.ttf) format('truetype');
	}
	@font-face {
		font-family: 'Mulish';
		font-style: normal;
		font-weight: 700;
		src: url('../src/fonts/Mulish-Bold.ttf) format('truetype');
	}
	body {
		font-family: 'Mulish', sans-serif;
	}
`;