def pytest_addoption(parser):
    parser.addoption(
        "--update-golden-files",
        action="store_true",
        default=False,
    )
