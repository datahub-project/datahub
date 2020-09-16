const emailAddress = 'nowhere@example.com';
const cc = 'nowhere1@example.com';
const bcc = 'nowhere-bcc@example.com';
const body = 'Message';
const subject = 'Email';
const base = 'mailto:';
const mailToEmail = `${base}${encodeURIComponent(emailAddress)}`;

export const emailMailToAsserts = [
  {
    expected: base,
    args: void 0,
    assertMsg: 'it should return a basic mailto: string without an email when no arguments are passed'
  },
  {
    expected: base,
    args: {},
    assertMsg: 'it should return a basic mailto: string without an email when an empty object is passed'
  },
  {
    expected: base,
    args: { to: '' },
    assertMsg:
      'it should return a basic mailto: string without an email when an object with only an empty string in the `to` field is passed'
  },
  {
    expected: `${mailToEmail}`,
    args: { to: emailAddress },
    assertMsg: 'it should return a mailto: string with an email when an object with only the `to` field is passed'
  },
  {
    expected: `${mailToEmail}?cc=${encodeURIComponent(cc)}`,
    args: { to: emailAddress, cc },
    assertMsg: 'it should return a mailto: string with an email and a cc query when to and cc are passed in'
  },
  {
    expected: `${mailToEmail}?cc=${encodeURIComponent(cc)}&subject=${encodeURIComponent(subject)}`,
    args: { to: emailAddress, cc, subject },
    assertMsg:
      'it should return a mailto: string with an email, subject, and a cc query when to, subject, and cc are passed in'
  },
  {
    expected: `${mailToEmail}?cc=${encodeURIComponent(cc)}&subject=${encodeURIComponent(
      subject
    )}&bcc=${encodeURIComponent(bcc)}`,
    args: { to: emailAddress, cc, subject, bcc },
    assertMsg:
      'it should return a mailto: string with an email, subject, bcc, and a cc query when to, subject, bcc, and cc are passed in'
  },
  {
    expected: `${mailToEmail}?cc=${encodeURIComponent(cc)}&subject=${encodeURIComponent(
      subject
    )}&bcc=${encodeURIComponent(bcc)}&body=${encodeURIComponent(body)}`,
    args: { to: emailAddress, cc, subject, bcc, body },
    assertMsg:
      'it should return a mailto: string with an email, subject, bcc, body, and a cc query when to, subject, bcc, body, and cc are passed in'
  }
];
