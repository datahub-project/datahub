interface IMockTimeStamp {
  policyModificationTime: number;
  suggestionModificationTime: number;
  __assertMsg__: string;
  __isRecent__: boolean;
}

const sevenDays: number = 7 * 24 * 60 * 60 * 1000;

const isRecent = ({ policyModificationTime, suggestionModificationTime }: IMockTimeStamp) =>
  !policyModificationTime ||
  (!!suggestionModificationTime && suggestionModificationTime - policyModificationTime >= sevenDays);

const mockTimeStamps = [
  {
    policyModificationTime: new Date('2017-10-08 06:58:53.0').getTime(),
    suggestionModificationTime: new Date('2017-10-15 06:58:53.0').getTime(),
    __assertMsg__: 'Suggestion time is exactly 7 days after policy modification date',
    get __isRecent__(): boolean {
      return isRecent(this);
    }
  },
  {
    policyModificationTime: new Date('2017-10-08 06:58:53.0').getTime(),
    suggestionModificationTime: new Date('2017-10-15 06:57:53.0').getTime(),
    __assertMsg__: 'Suggestion time is slightly less than 7 days after policy modification date',
    get __isRecent__(): boolean {
      return isRecent(this);
    }
  },
  {
    policyModificationTime: new Date('2017-10-08 06:58:53.0').getTime(),
    suggestionModificationTime: new Date('2017-10-14 06:58:53.0').getTime(),
    __assertMsg__: 'Suggestion time is 6 days after policy modification date',
    get __isRecent__(): boolean {
      return isRecent(this);
    }
  },
  {
    policyModificationTime: new Date('2017-10-08 06:58:53.0').getTime(),
    suggestionModificationTime: new Date('2016-10-15 06:58:53.0').getTime(),
    __assertMsg__: 'Suggestion time is before policy modification date',
    get __isRecent__(): boolean {
      return isRecent(this);
    }
  },
  {
    policyModificationTime: new Date('2017-10-08 06:58:53.0').getTime(),
    suggestionModificationTime: new Date('2017-11-15 06:58:53.0').getTime(),
    __assertMsg__: 'Suggestion time is greater than minimum interval since policy modification date',
    get __isRecent__(): boolean {
      return isRecent(this);
    }
  },
  {
    policyModificationTime: 0,
    suggestionModificationTime: new Date('2017-11-15 06:58:53.0').getTime(),
    __assertMsg__: 'Policy modification time does not exist',
    get __isRecent__(): boolean {
      return isRecent(this);
    }
  },
  {
    policyModificationTime: new Date('2017-11-15 06:58:53.0').getTime(),
    suggestionModificationTime: 0,
    __assertMsg__: 'Suggestion modification time does not exist',
    get __isRecent__(): boolean {
      return isRecent(this);
    }
  },
  {
    policyModificationTime: 0,
    suggestionModificationTime: 0,
    __assertMsg__: 'Suggestion and policy modification time does not exist',
    get __isRecent__(): boolean {
      return isRecent(this);
    }
  }
];

export { mockTimeStamps };
