import pandas as pd


def gen_hourly_files(**kwargs):
    date0 = kwargs["cycle"].date  # provided by the context
    date1 = date0.add(kwargs["cycles_freq"])
    path_format = kwargs["path_format"]  # provided by the tasks.cfg file
    hours = pd.date_range(date0, date1, freq="1h", inclusive="left")
    paths = []
    for time in hours:
        paths.append(path_format.format(time=time, **kwargs))
    return paths


ARTIFACTS_GENERATORS = {"gen_hourly_files": gen_hourly_files}
