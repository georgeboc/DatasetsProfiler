from pyspark.storagelevel import StorageLevel


class PersistentCheckpointer:
    def checkpoint(self, data_frame):
        data_frame.rdd.persist(StorageLevel.MEMORY_AND_DISK)
        return data_frame
