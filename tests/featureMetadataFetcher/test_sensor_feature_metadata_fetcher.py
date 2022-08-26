"""
    Test module for sensor feature metadata fetcher
"""
import sys
import pprint

sys.path.append("../../src")
print(sys.path)
import os
import pytest
from featureMetadataFetcher import FeatureMetadataFetcher, SensorMetadataFetcher
from pymongo_inmemory import MongoClient
from utils.feature_util import mongo_to_dict


class TestSensorMetadataFether:
    @pytest.fixture(scope="function", autouse=False)
    def test_source(self):
        client = MongoClient()
        yield client
        client.close()

    @pytest.fixture(autouse=True)
    def pymongoim_os_env(self):
        if os.environ.get("PYMONGOIM__MONGOD_PORT") is None:
            os.environ["PYMONGOIM__MONGOD_PORT"] = "32154"
        if os.environ.get("PYMONGOIM__OPERATING_SYSTEM") is None:
            os.environ["PYMONGOIM__OPERATING_SYSTEM"] = "ubuntu"
        if os.environ.get("PYMONGOIM__OS_VERSION") is None:
            os.environ["PYMONGOIM__OS_VERSION"] = "18"
        yield
        # os.environ.pop("PYMONGOIM__MONGOD_PORT")
        # os.environ.pop("PYMONGOIM__OPERATING_SYSTEM")
        # os.environ.pop("PYMONGOIM__OS_VERSION")

    @pytest.fixture(scope="session", autouse=False)
    def mock_positions(self):
        yield [
            {"pos_id": 1, "pos_code": "CD", "pos_dtl": "A street", "pos_name": "CD"},
            {"pos_id": 3, "pos_code": "AB", "pos_dtl": "B street", "pos_name": "AB"},
        ]

    @pytest.fixture(scope="session", autouse=False)
    def mock_sensors(self):
        yield [
            {
                "rstart": 0.0,
                "rlev2": 0.0,
                "rlev3": 0.0,
                "rlev5": None,
                "rlev7": None,
                "rend": 0.0,
                "range_type": None,
                "rlev1": 0.0,
                "rlev4": 0.0,
                "rlev6": None,
                "rlev8": None,
                "ss_id": 4,
                "position": {
                    "pos_code": "CD",
                    "pos_dtl": "A street",
                    "pos_name": "CD",
                    "pos_id": 1,
                },
                "type": {
                    "type_code": "T",
                    "type_name": "Temperature",
                    "unit": "ºC",
                    "type_id": 2,
                    "type_color_code": "#f44336",
                },
            }
        ]

    def test_fetch_metadata(self, test_source, mock_positions, mock_sensors):
        # Given (Scenario 1)
        test_source["feature"]["position"].insert_many(mock_positions)
        test_source["feature"]["sensor"].insert_many(mock_sensors)
        fetcher: FeatureMetadataFetcher = SensorMetadataFetcher()
        fetcher.get_or_create_conn(
            conn_info={"METADATA_HOST": "localhost", "METADATA_PORT": "32154"}
        )

        # When (Scenario 1)
        sensors, positions = fetcher.fetch_metadata()
        fetcher.close_conn()
        # Then (Scenario 1)
        for sensor in sensors:
            assert self.__is_mongo_object_in_list_of_dict(sensor, mock_sensors)
        for position in positions:
            assert self.__is_mongo_object_in_list_of_dict(position, mock_positions)

    def __is_mongo_object_in_list_of_dict(self, obj, li):
        dict_obj = mongo_to_dict(obj)
        result = len(list(filter(lambda x: dict_obj.items() <= x.items(), li)))
        return result > 0
