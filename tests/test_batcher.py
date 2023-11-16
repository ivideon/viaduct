from ivideon.viaduct.viaduct.handlers import Batcher


def test_collect_batch() -> None:
    # arrange
    batch_size = 4
    sut = Batcher(batch_size=batch_size)
    sut.on_start()

    # act
    result = [sut.handle(stream_id='dummy', data=i) for i in range(batch_size)]

    # assert
    assert result.pop() == (list(range(batch_size)), ['dummy'] * batch_size)

    for i in range(batch_size - 1):
        assert result.pop() is None
