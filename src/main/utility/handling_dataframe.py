
from pyspark.sql.functions import lit, col, to_date, to_timestamp


class DataframeOperation:
    def __init__(self):
        pass

    def make_dict(self,data_df):

        convert_dict = {}
        for field in data_df.schema.fields:
            col_name = field.name.strip().lower()
            col_type = field.dataType.simpleString().lower()

            convert_dict[col_name] = {
                "type": col_type,
                "nullable": field.nullable
            }

        return convert_dict

    def add_default_value(self, colm_data_type = "string",colm_nullable = False):
        if colm_nullable == True:
            return lit(None)

        default_value = {
                                "string": lit(""),
                                "integer": lit(0),
                                "double": lit(0.0),
                                "float": lit(0.0),
                                "boolean": lit(False),
                                "date": lit("1970-01-01"),
                                "timestamp": lit("1970-01-01 00:00:00")
                            }
        if colm_data_type in default_value:
            return default_value[colm_data_type]
        return lit("")

    def convert_type(self, data_df, key, expected_type, actual_type):
        convert_mapping = {
            "integer": "int",
            "int": "int",
            "bigint": "int",
            "float": "double",
            "decimal": "double",
            "double": "double",
            "string": "string",
            "boolean": "boolean",
            "date": "date",
            "timestamp": "timestamp"
        }

        actual_type = convert_mapping.get(actual_type, actual_type)
        expected_type = convert_mapping.get(expected_type, expected_type)

        # Only cast if needed
        if actual_type != expected_type:
            if expected_type == "date":
                # Try common formats, default to yyyy-MM-dd
                data_df = data_df.withColumn(
                    key,
                    to_date(col(key), "yyyy-MM-dd")  # adjust if needed
                )
            elif expected_type == "timestamp":
                data_df = data_df.withColumn(
                    key,
                    to_timestamp(col(key), "yyyy-MM-dd HH:mm:ss")  # adjust if needed
                )
            else:
                data_df = data_df.withColumn(key, col(key).cast(expected_type))

        return data_df