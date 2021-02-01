class TextReader:
    def read(self, spark_context, filename):
        return spark_context.textFile(filename).filter(bool).map(lambda line: (line,))