from abc import abstractmethod
from enum import Enum
from itertools import cycle
from multiprocessing.shared_memory import SharedMemory
from typing import Any, Iterator, List, Optional, Tuple

import numpy as np

from ivideon.viaduct.viaduct import Handler


class ImagesSharedRingBufferMixin(Handler):
    """Кольцевой буфер для хранения изображений в разделяемой памяти."""

    ImageDtype = Enum(value='ImageDtype', names=['float32', 'uint8'])

    def __init__(self, name: str, create: bool, buffer_size: int, image_shape: List[int], image_dtype: str):
        """
        Args:
            name: имя разделяемой памяти
            create: создавать ли разделяемую память?
            buffer_size: размер буфера в количестве изображений
            image_shape: размерность изображения
            image_dtype: тип данных изображения
        """
        self._name = name
        self._create = create
        self._size: int = int(buffer_size * np.prod(image_shape) * np.dtype(image_dtype).itemsize)
        self._shape: List[int] = [buffer_size] + image_shape
        self._image_shape: List[int] = image_shape
        self._dtype: str = self.ImageDtype[image_dtype.lower()].name

        self._shared_memory: Optional[SharedMemory] = None
        self._buffer: Optional[np.ndarray] = None
        self._index: Iterator[int] = cycle(range(buffer_size))

    def on_start(self) -> None:
        self._shared_memory = SharedMemory(name=self._name, create=self._create, size=self._size)
        self._buffer = np.ndarray(shape=self._shape, dtype=self._dtype, buffer=self._shared_memory.buf)

    @abstractmethod
    def handle(self, *args, **kwargs) -> Any:
        raise NotImplementedError

    def on_exit(self) -> None:
        del self._buffer
        self._shared_memory.close()
        try:
            self._shared_memory.unlink()
        except FileNotFoundError:
            pass


class ImagesSharedRingBufferWriter(ImagesSharedRingBufferMixin):
    """Запись изображений в кольцевой буфер."""
    def handle(self, image: np.ndarray) -> int:
        index: int = next(self._index)
        self._buffer[index] = image[:]
        return index


class ImagesSharedRingBufferReader(ImagesSharedRingBufferMixin):
    """Чтение изображений из кольцевого буфера."""
    def handle(self, start: int, end: int) -> np.ndarray:
        return self._buffer[start:end]


class Batcher(Handler):
    """Отвечает за формирование батча данных."""
    def __init__(self, batch_size: int):
        self._batch_size = batch_size
        self._data: Optional[List[Any]] = None
        self._stream_ids: Optional[List[Any]] = None

    def on_start(self) -> None:
        self._data = []
        self._stream_ids = []

    def _clear(self) -> None:
        self._data.clear()
        self._stream_ids.clear()

    def handle(self, stream_id: Any, data: Any) -> Optional[Tuple[List[Any], List[Any]]]:
        self._data.append(data)
        self._stream_ids.append(stream_id)

        if len(self._data) == len(self._stream_ids) == self._batch_size:
            batch = (self._data.copy(), self._stream_ids.copy())
            self._clear()
            return batch

        return None

    def on_exit(self) -> None:
        self._clear()
