from datahub.utilities.delayed_iter import delayed_iter


def test_delayed_iter():
    events = []

    def maker():
        for i in range(4):
            events.append(("add", i))
            yield i

    for i in delayed_iter(maker(), 2):
        events.append(("remove", i))

    assert events == [
        ("add", 0),
        ("add", 1),
        ("add", 2),
        ("remove", 0),
        ("add", 3),
        ("remove", 1),
        ("remove", 2),
        ("remove", 3),
    ]
