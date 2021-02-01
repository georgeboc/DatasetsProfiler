class DataFrameTransformerDispatcher:
    def __init__(self, data_frame_transformer_classes):
        self._data_frame_transformer_classes = data_frame_transformer_classes

    def dispatch(self, schema_transformer_class_name):
        return self._data_frame_transformer_classes[schema_transformer_class_name]()