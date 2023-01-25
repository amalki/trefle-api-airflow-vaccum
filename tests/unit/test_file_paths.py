# from dags.dependencies import parameters
from dags.dependencies.helpers import (
    build_file_name_datetime,
    list_files_in_folder,
    get_matching_file_path,
    read_json_schema,
)


from datetime import datetime

class TestFilePaths:
    def test_build_file_name_datetime(self):

        # Given
        dt = datetime.today().strftime("%Y%m%dT%H%M%S")
        file_name = "file.txt"

        # When
        new_file_name = build_file_name_datetime(file_name, dt)

        # Then
        assert f"file_{dt}.txt" == new_file_name
