from typing import TYPE_CHECKING

try:
    # Via https://stackoverflow.com/a/65147676
    if not TYPE_CHECKING and __sphinx_build__:
        IS_SPHINX_BUILD = True

except NameError:
    IS_SPHINX_BUILD = False
