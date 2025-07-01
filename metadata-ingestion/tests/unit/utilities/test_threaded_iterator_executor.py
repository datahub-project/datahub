from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor


def test_threaded_iterator_executor():
    def table_of(i):
        for j in range(1, 11):
            yield f"{i}x{j}={i * j}"

    assert {
        res
        for res in ThreadedIteratorExecutor.process(
            table_of, [(i,) for i in range(1, 30)], max_workers=2
        )
    } == {x for i in range(1, 30) for x in table_of(i)}
