from test import support
from test.support import import_helper
import unittest
import warnings


# Skip test if nis module does not exist.
with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    nis = import_helper.import_module('nis')


class NisTests(unittest.TestCase):
    def test_maps(self):
        try:
            maps = nis.maps()
        except nis.error as msg:
            # NIS is probably not active, so this test isn't useful
            self.skipTest(str(msg))
        try:
            # On some systems, this map is only accessible to the
            # super user
            maps.remove("passwd.adjunct.byname")
        except ValueError:
            pass

        done = 0
        for nismap in maps:
            mapping = nis.cat(nismap)
            for k, v in mapping.items():
                if not k:
                    continue
                if nis.match(k, nismap) != v:
                    self.fail("NIS match failed for key `%s' in map `%s'" % (k, nismap))
                else:
                    # just test the one key, otherwise this test could take a
                    # very long time
                    done = 1
                    break
            if done:
                break

if __name__ == '__main__':
    unittest.main()
