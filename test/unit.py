import pytest

from lib.common import quick_sort, merge_int_iterables
from lib.data import DataReader


@pytest.mark.parametrize('chunks, expected', (
    (['10, 20', ''], [[10, 20]]),
    (['10000, 200', '00, 30000', ''], [[10000], [20000], [30000]]),
    (['10, 20, 30', ', 40', ''], [[10, 20], [30, 40]]),
))
def test_data_reader_read_from_file(mocker, chunks, expected):
    file = mocker.Mock()
    file.read.side_effect = chunks
    result = DataReader('work_dir')._read_from_file(file, 10)
    assert list(result) == expected


def test_quick_sort():
    result = quick_sort([-1, 2, -3, -8, 11, 10, 7, 12, 1, 4, 14, 0, -7, 3, 8,
                         13, -4, 15, -9, 6, 16, 9, 5, 5, 1, -6, -2, -10, -5, 17])
    assert result == [-10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 1, 2, 3,
                      4, 5, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]


def test_merge_int_iterables():
    result = merge_int_iterables((
        [1, 4, 8, 12],
        [2, 2, 3, 5],
        [0, 7, 8, 11],
        [-3, 1, 2, 4],
    ))
    assert list(result) == [-3, 0, 1, 1, 2, 2, 2, 3, 4, 4, 5, 7, 8, 8, 11, 12]
