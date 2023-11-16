import multiprocessing as mp
import time
from random import random
from typing import List

import pytest

from ivideon.viaduct.viaduct import AnalyticsFacade


def worker(worker_id: int, threshold: float, analytics: AnalyticsFacade, output_queue: mp.Queue) -> None:
    stream_id: str = str(worker_id)
    analytics.init_stream(stream_id=stream_id, heuristic_threshold=threshold)
    analytics.process_frame(stream_id=stream_id, frame=[])
    time.sleep(1.0)
    output_queue.put(analytics.get_events(stream_id=stream_id))


expected_result: str = """Dummy analytics flow stream_id = {stream_id}
I am PreProcessor with default threshold 0.5
I am Inference with default threshold 0.5
I am PostProcessor with default threshold 0.5
I am Heuristic with stream threshold {threshold:.1f}"""


@pytest.mark.parametrize('num_workers', [1, 2, 4])
def test_dummy_analytics_viaduct_flow(num_workers, dummy_analytics: AnalyticsFacade) -> None:
    # arrange
    workers_thresholds: List[float] = [random() for _ in range(num_workers)]
    queue = mp.Queue()
    processes: List[mp.Process] = []
    # act
    for worker_id, threshold in enumerate(workers_thresholds):
        processes.append(mp.Process(target=worker, args=(worker_id, threshold, dummy_analytics, queue)))
        processes[-1].start()
        time.sleep(1.0)

    events: List[List[str]] = [queue.get() for _ in range(num_workers)]

    # assert
    assert queue.empty()
    assert events == [[expected_result.format(stream_id=i, threshold=t)] for i, t in enumerate(workers_thresholds)]

    # cleanup
    for process in processes:
        process.join()

    dummy_analytics.stop()
