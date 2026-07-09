

# import io
# import pandas as pd

# from pipeline.transform import transform_mongodb


# class FakeObject:
#     def __init__(self, name):
#         self.object_name = name


# class FakeResponse:
#     def __init__(self, data):
#         self.data = data

#     def read(self):
#         return self.data

#     def close(self):
#         pass

#     def release_conn(self):
#         pass


# class FakeMinio:

#     def __init__(self, parquet_bytes):
#         self.parquet_bytes = parquet_bytes


#     def list_objects(
#         self,
#         bucket,
#         prefix,
#         recursive
#     ):
#         return [
#             FakeObject(
#                 "raw/mongodb/customer_sessions/file.parquet"
#             )
#         ]


#     def get_object(
#         self,
#         bucket,
#         object_name
#     ):
#         return FakeResponse(
#             self.parquet_bytes
#         )


# def test_customer_sessions_transformation():

#     # create dummy MongoDB dataframe
#     df = pd.DataFrame(
#         {
#             "customer_id": [1, 1],
#             "product_id": [100, 100],
#             "quantity": [2, 2],
#             "started_at": [
#                 "2026-01-01",
#                 "2026-01-01"
#             ],
#             "ended_at": [
#                 "2026-01-02",
#                 "2026-01-02"
#             ],
#             "event_time": [
#                 "2026-01-01",
#                 "2026-01-01"
#             ],

#             "device": [
#                 "mobile",
#                 "mobile"
#             ],

#             "events": [
#                 [
#                     {
#                         "type": "phone",
#                         "os": "android"
#                     }
#                 ],
#                 [
#                     {
#                         "type": "phone",
#                         "os": "android"
#                     }
#                 ]
#             ]
#         }
#     )


#     # convert dataframe to parquet bytes
#     buffer = io.BytesIO()

#     df.to_parquet(buffer)

#     parquet_bytes = buffer.getvalue()


#     # fake MinIO
#     client = FakeMinio(
#         parquet_bytes
#     )


#     # run YOUR real function
#     result = transform_mongodb(
#         client,
#         "test_bucket"
#     )


#     # validation checks

#     assert "device_type" in result.columns

#     assert "device_os" in result.columns


#     # check datetime conversion
#     assert pd.api.types.is_datetime64_any_dtype(
#         result["started_at"]
#     )


#     # check duplicate removal
#     assert len(result) == 1


#     # check renamed values
#     assert result.iloc[0]["device_type"] == "phone"
#     assert result.iloc[0]["device_os"] == "android"