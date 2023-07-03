from datahub.ingestion.source.s3.source import partitioned_folder_comparator


def test_partition_comparator_numeric_folder_name():
    folder1 = "3"
    folder2 = "12"
    assert partitioned_folder_comparator(folder1, folder2) == -1


def test_partition_comparator_numeric_folder_name2():
    folder1 = "12"
    folder2 = "3"
    assert partitioned_folder_comparator(folder1, folder2) == 1


def test_partition_comparator_string_folder():
    folder1 = "myfolderB"
    folder2 = "myFolderA"
    assert partitioned_folder_comparator(folder1, folder2) == 1


def test_partition_comparator_string_same_folder():
    folder1 = "myFolderA"
    folder2 = "myFolderA"
    assert partitioned_folder_comparator(folder1, folder2) == 0


def test_partition_comparator_with_numeric_partition():
    folder1 = "year=3"
    folder2 = "year=12"
    assert partitioned_folder_comparator(folder1, folder2) == -1


def test_partition_comparator_with_padded_numeric_partition():
    folder1 = "year=03"
    folder2 = "year=12"
    assert partitioned_folder_comparator(folder1, folder2) == -1


def test_partition_comparator_with_equal_sign_in_name():
    folder1 = "month=12"
    folder2 = "year=0"
    assert partitioned_folder_comparator(folder1, folder2) == -1


def test_partition_comparator_with_string_partition():
    folder1 = "year=year2020"
    folder2 = "year=year2021"
    assert partitioned_folder_comparator(folder1, folder2) == -1
