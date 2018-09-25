const emailAddress = 'nowhere@example.com';
const cc = 'nowhere1@example.com';
const bcc = 'nowhere-bcc@example.com';
const body = 'Message';
const subject = 'Email';
const base = 'mailto:';
const mailToEmail = `${base}${emailAddress}`;
const emailMailToAsserts = [
  {
    __expected__: base,
    __args__: void 0,
    __assert_msg__: 'it should return a basic mailto: string without an email when no arguments are passed'
  },
  {
    __expected__: base,
    __args__: {},
    __assert_msg__: 'it should return a basic mailto: string without an email when an empty object is passed'
  },
  {
    __expected__: base,
    __args__: { to: '' },
    __assert_msg__:
      'it should return a basic mailto: string without an email when an object with only an empty string in the `to` field is passed'
  },
  {
    __expected__: `${mailToEmail}`,
    __args__: { to: emailAddress },
    __assert_msg__: 'it should return a mailto: string with an email when an object with only the `to` field is passed'
  },
  {
    __expected__: `${mailToEmail}?cc=${encodeURIComponent(cc)}`,
    __args__: { to: emailAddress, cc },
    __assert_msg__: 'it should return a mailto: string with an email and a cc query when to and cc are passed in'
  },
  {
    __expected__: `${mailToEmail}?cc=${encodeURIComponent(cc)}&subject=${encodeURIComponent(subject)}`,
    __args__: { to: emailAddress, cc, subject },
    __assert_msg__:
      'it should return a mailto: string with an email, subject, and a cc query when to, subject, and cc are passed in'
  },
  {
    __expected__: `${mailToEmail}?cc=${encodeURIComponent(cc)}&subject=${encodeURIComponent(
      subject
    )}&bcc=${encodeURIComponent(bcc)}`,
    __args__: { to: emailAddress, cc, subject, bcc },
    __assert_msg__:
      'it should return a mailto: string with an email, subject, bcc, and a cc query when to, subject, bcc, and cc are passed in'
  },
  {
    __expected__: `${mailToEmail}?cc=${encodeURIComponent(cc)}&subject=${encodeURIComponent(
      subject
    )}&bcc=${encodeURIComponent(bcc)}&body=${encodeURIComponent(body)}`,
    __args__: { to: emailAddress, cc, subject, bcc, body },
    __assert_msg__:
      'it should return a mailto: string with an email, subject, bcc, body, and a cc query when to, subject, bcc, body, and cc are passed in'
  }
];

export { emailMailToAsserts };
