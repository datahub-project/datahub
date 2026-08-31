NOTE: these integration tests only run against Db2 for LUW, as that
is the only variant of Db2 that is freely-available and runs on most
users' computers.

For testing against Db2 for IBM i (AS/400), a real server, such as
that provided by [PUB400.COM](https://pub400.com/), must be used. In the
interest of not overburdening the public resource that is PUB400.COM, automated
tests against it are not performed. To test against PUB400.com, you can
sign up for a new account, connect with a 5250 telnet client such as
[tn5250](https://github.com/tn5250/tn5250) to reset your password, then
pass `"PUB400.COM"` as the `host_port` and any arbitrary string as `database`.
MCE golden files are not included because PUB400.COM creates separate schemas
per user, so container URNs will differ.

If you find a freely available Db2 for z/OS server to test against,
please update this note.
