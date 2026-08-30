import json
import os

from pyflink.table import EnvironmentSettings, TableEnvironment


APPLICATION_PROPERTIES_FILE_PATH = (
    "/etc/flink/application_properties.json"
)

PROPERTY_GROUP_ID = "consumer.config.0"


def load_application_properties():
    """
    Lee las Runtime Properties generadas por
    AWS Managed Service for Apache Flink.
    """

    if not os.path.isfile(
        APPLICATION_PROPERTIES_FILE_PATH
    ):
        raise FileNotFoundError(
            "No se encontró "
            f"{APPLICATION_PROPERTIES_FILE_PATH}"
        )

    with open(
        APPLICATION_PROPERTIES_FILE_PATH,
        "r",
        encoding="utf-8"
    ) as file:

        return json.load(file)


def get_property_map(
    properties,
    property_group_id
):
    """
    Obtiene el PropertyMap correspondiente
    al PropertyGroupId indicado.
    """

    for prop in properties:

        if (
            prop.get("PropertyGroupId")
            == property_group_id
        ):
            return prop.get(
                "PropertyMap",
                {}
            )

    raise ValueError(
        "No se encontró el PropertyGroupId: "
        f"{property_group_id}"
    )


def main():

    # -----------------------------------------------------
    # 1. RUNTIME PROPERTIES
    # -----------------------------------------------------

    properties = load_application_properties()

    consumer_config = get_property_map(
        properties,
        PROPERTY_GROUP_ID
    )

    stream_name = consumer_config[
        "stream.name"
    ]

    aws_region = consumer_config[
        "aws.region"
    ]

    stream_init_position = consumer_config.get(
        "flink.stream.initpos",
        "LATEST"
    )


    # -----------------------------------------------------
    # 2. ENTORNO PYFLINK
    # -----------------------------------------------------

    settings = (
        EnvironmentSettings
        .in_streaming_mode()
    )

    table_env = TableEnvironment.create(
        environment_settings=settings
    )

    configuration = (
        table_env
        .get_config()
        .get_configuration()
    )

    configuration.set_string(
        "execution.checkpointing.interval",
        "60s"
    )


    # -----------------------------------------------------
    # 3. SOURCE KINESIS
    # -----------------------------------------------------

    table_env.execute_sql(
        f"""
        CREATE TABLE urban_sensors (

            sensor_id STRING,

            temperature DOUBLE,

            humidity DOUBLE,

            air_quality_index INT,

            event_time TIMESTAMP(3),

            WATERMARK FOR event_time
                AS event_time
                - INTERVAL '5' SECOND

        )
        WITH (

            'connector' = 'kinesis',

            'stream' = '{stream_name}',

            'aws.region' = '{aws_region}',

            'scan.stream.initpos'
                = '{stream_init_position}',

            'format' = 'json',

            'json.timestamp-format.standard'
                = 'SQL'

        )
        """
    )


    # -----------------------------------------------------
    # 4. AGREGACIÓN STATEFUL
    # -----------------------------------------------------

    result = table_env.sql_query(
        """
        SELECT

            sensor_id,

            window_start,

            window_end,

            AVG(temperature)
                AS avg_temperature,

            AVG(
                CAST(
                    air_quality_index
                    AS DOUBLE
                )
            )
                AS avg_air_quality_index,

            COUNT(*)
                AS event_count

        FROM TABLE(

            TUMBLE(

                TABLE urban_sensors,

                DESCRIPTOR(event_time),

                INTERVAL '1' MINUTE

            )

        )

        GROUP BY

            sensor_id,

            window_start,

            window_end
        """
    )


    # -----------------------------------------------------
    # 5. OUTPUT
    # -----------------------------------------------------

    result.execute().print()


if __name__ == "__main__":
    main()