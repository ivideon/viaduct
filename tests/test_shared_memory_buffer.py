from pathlib import Path

import numpy as np
import pytest

from ivideon.viaduct.viaduct.handlers import (
    ImagesSharedRingBufferReader as Reader, ImagesSharedRingBufferWriter as Writer,
)


@pytest.mark.parametrize('dtype', ['float32', 'uint8'])
def test_write_and_read_image_to_shared_memory_ring_buffer(dtype: str) -> None:
    # arrange
    shape = [100, 100, 3]
    dummy_image = np.random.randint(low=0, high=255, size=shape, dtype=np.uint8).astype(dtype)

    writer = Writer(name='test', create=True, buffer_size=1, image_shape=shape, image_dtype=dtype)
    writer.on_start()

    reader = Reader(name='test', create=False, buffer_size=1, image_shape=shape, image_dtype=dtype)
    reader.on_start()

    # act
    index: int = writer.handle(image=dummy_image)
    result: np.ndarray = reader.handle(start=index, end=index + 1)

    # assert
    assert index == 0
    assert np.all(result == dummy_image)
    assert Path('/dev/shm/test').stat().st_size == np.prod(shape) * np.dtype(dtype).itemsize

    # cleanup
    writer.on_exit()
    reader.on_exit()

    assert not Path('/dev/shm/test').exists()
