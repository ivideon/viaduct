# Viaduct

`Viaduct` - it is microframework for organizing `flow` video analytics when processing multiple CCTV streams.

Base concepts of `Viaduct` were taken and adapted for video analytics from [Aqueduct](https://github.com/avito-tech/aqueduct).

Key features:
- easy integration into video stream decoding workers
- "lazy" initialization of handlers with individual settings for each stream
- `flow` analytics configuration through `yaml` file
- frames transfer between handlers via `shared memory`
- data batching

`Viaduct` has two dependencies:
- [Hydra](https://pypi.org/project/hydra-core/) - allow to configure `flow` analytics and "lazy" initialization of handlers
- [NumPy](https://pypi.org/project/numpy/) - allow to create a "wrapper" over `shared memory` for frames transfer between handlers

Example of dummy analytics with `Viaduct` can be found in `tests/test_dummy_analytics_viaduct_flow.py`.

## Installation
```bash
mkdir ivideon
touch ivideon/__init__.py
git clone git@gitlab.extcam.com:ivideon/va/shorts/viaduct.git ivideon/
python3 -m venv venv
source venv/bin/activate
pip3 install -U pip
pip3 install -r ivideon/viaduct/requirements-dev.txt

make test
```