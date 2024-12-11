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