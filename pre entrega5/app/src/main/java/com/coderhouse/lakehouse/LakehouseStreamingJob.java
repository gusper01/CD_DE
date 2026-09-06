package com.coderhouse.lakehouse;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.apache.flink.util.Collector;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;


/**
 * Pre-entrega 5
 *
 * Pipeline:
 *
 * Kinesis
 *   -> JSON
 *   -> SensorEvent
 *   -> event time + watermark
 *   -> ventana tumbling de 1 minuto por sensor
 *   -> AVG temperature
 *   -> AVG air_quality_index
 *   -> COUNT
 *   -> Iceberg
 *   -> AWS Glue Data Catalog
 *   -> S3 Lakehouse
 */
public class LakehouseStreamingJob {

    private static final String CONSUMER_GROUP =
            "consumer.config.0";

    private static final String LAKEHOUSE_GROUP =
            "lakehouse.config.0";


    public static void main(String[] args) throws Exception {

        // =====================================================
        // 1. FLINK ENVIRONMENT
        // =====================================================

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        /*
         * Iceberg realiza los commits asociados a checkpoints.
         * Terraform también configura checkpointing cada 60 s.
         */
        env.enableCheckpointing(60_000);


        // =====================================================
        // 2. AWS MANAGED FLINK RUNTIME PROPERTIES
        // =====================================================

        Map<String, Properties> applicationProperties =
                KinesisAnalyticsRuntime.getApplicationProperties();

        if (applicationProperties == null) {
            throw new RuntimeException(
                    "KinesisAnalyticsRuntime.getApplicationProperties() devolvio null"
            );
        }


        Properties consumerConfig =
                applicationProperties.get(CONSUMER_GROUP);

        Properties lakehouseConfig =
                applicationProperties.get(LAKEHOUSE_GROUP);


        if (consumerConfig == null) {
            throw new RuntimeException(
                    "No se encontro el grupo "
                            + CONSUMER_GROUP
                            + ". Grupos disponibles: "
                            + applicationProperties.keySet()
            );
        }


        if (lakehouseConfig == null) {
            throw new RuntimeException(
                    "No se encontro el grupo "
                            + LAKEHOUSE_GROUP
                            + ". Grupos disponibles: "
                            + applicationProperties.keySet()
            );
        }


        String streamName =
                consumerConfig.getProperty("stream.name");

        String streamArn =
                consumerConfig.getProperty("stream.arn");

        String awsRegion =
                consumerConfig.getProperty("aws.region");

        String streamInitPosition =
                consumerConfig.getProperty(
                        "flink.stream.initpos",
                        "LATEST"
                );


        String glueDatabase =
                lakehouseConfig.getProperty("glue.database");

        String lakehouseBucket =
                lakehouseConfig.getProperty("lakehouse.bucket");


        if (streamName == null
                || awsRegion == null
                || glueDatabase == null
                || lakehouseBucket == null) {

            throw new RuntimeException(
                    "Faltan propiedades obligatorias. "
                            + "consumer.config.0="
                            + consumerConfig.stringPropertyNames()
                            + ", lakehouse.config.0="
                            + lakehouseConfig.stringPropertyNames()
            );
        }


        /*
         * El nuevo KinesisStreamsSource 5.x requiere ARN,
         * no solamente el nombre del stream.
         *
         * Más adelante agregaremos stream.arn a Terraform.
         */
        if (streamArn == null || streamArn.isBlank()) {

            throw new RuntimeException(
                    "Falta consumer.config.0 -> stream.arn. "
                            + "El conector Kinesis 5.x requiere el ARN "
                            + "del stream. stream.name actual="
                            + streamName
            );
        }


        // =====================================================
        // 3. KINESIS SOURCE
        // =====================================================

        org.apache.flink.configuration.Configuration sourceConfig =
                new org.apache.flink.configuration.Configuration();

        sourceConfig.setString(
                "aws.region",
                awsRegion
        );


        KinesisSourceConfigOptions.InitialPosition initialPosition;

        try {

            initialPosition =
                    KinesisSourceConfigOptions.InitialPosition.valueOf(
                            streamInitPosition.toUpperCase(Locale.ROOT)
                    );

        } catch (IllegalArgumentException e) {

            throw new RuntimeException(
                    "Valor invalido para flink.stream.initpos: "
                            + streamInitPosition,
                    e
            );
        }


        sourceConfig.set(
                KinesisSourceConfigOptions.STREAM_INITIAL_POSITION,
                initialPosition
        );


        KinesisStreamsSource<String> source =
                KinesisStreamsSource.<String>builder()
                        .setStreamArn(streamArn)
                        .setSourceConfig(sourceConfig)
                        .setDeserializationSchema(
                                new SimpleStringSchema()
                        )
                        .build();


        DataStream<String> rawEvents =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "kinesis-source",
                        TypeInformation.of(String.class)
                );


        // =====================================================
        // 4. JSON -> SENSOR EVENT
        // =====================================================

        DataStream<SensorEvent> sensorEvents =
                rawEvents
                        .map(new SensorEventParser())
                        .name("parse-json")
                        .assignTimestampsAndWatermarks(

                                WatermarkStrategy
                                        .<SensorEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(5)
                                        )
                                        .withTimestampAssigner(
                                                (event, recordTimestamp) ->
                                                        event.eventTimeMillis
                                        )
                                        .withIdleness(
                                                Duration.ofSeconds(30)
                                        )
                        );


        // =====================================================
        // 5. EVENT-TIME WINDOW
        // =====================================================

        DataStream<AggregatedMetric> aggregated =
                sensorEvents

                        .keyBy(event -> event.sensorId)

                        .window(
                                TumblingEventTimeWindows.of(
                                        Time.minutes(1)
                                )
                        )

                        .aggregate(
                                new SensorAggregateFunction(),
                                new AddWindowInformation()
                        )

                        .name("sensor-window-aggregation");


        // =====================================================
        // 6. ICEBERG + GLUE CATALOG
        // =====================================================

        Map<String, String> catalogProperties =
                new HashMap<>();

        catalogProperties.put(
                "type",
                "iceberg"
        );

        catalogProperties.put(
                "catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog"
        );

        catalogProperties.put(
                "warehouse",
                "s3://" + lakehouseBucket + "/lakehouse/"
        );

        catalogProperties.put(
                "io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO"
        );


        CatalogLoader catalogLoader =
                CatalogLoader.custom(

                        "glue_catalog",

                        catalogProperties,

                        new Configuration(),

                        "org.apache.iceberg.aws.glue.GlueCatalog"
                );


        TableIdentifier tableId =
                TableIdentifier.of(
                        glueDatabase,
                        "sensor_events"
                );


        Catalog catalog =
                catalogLoader.loadCatalog();


        // =====================================================
        // 7. ICEBERG TABLE
        // =====================================================

        if (!catalog.tableExists(tableId)) {

            Schema schema =
                    new Schema(

                            Types.NestedField.required(
                                    1,
                                    "sensor_id",
                                    Types.StringType.get()
                            ),

                            Types.NestedField.required(
                                    2,
                                    "window_start",
                                    Types.TimestampType.withoutZone()
                            ),

                            Types.NestedField.required(
                                    3,
                                    "window_end",
                                    Types.TimestampType.withoutZone()
                            ),

                            Types.NestedField.required(
                                    4,
                                    "avg_temperature",
                                    Types.DoubleType.get()
                            ),

                            Types.NestedField.required(
                                    5,
                                    "avg_air_quality_index",
                                    Types.DoubleType.get()
                            ),

                            Types.NestedField.required(
                                    6,
                                    "event_count",
                                    Types.LongType.get()
                            ),

                            Types.NestedField.required(
                                    7,
                                    "event_date",
                                    Types.DateType.get()
                            )
                    );


            PartitionSpec partitionSpec =
                    PartitionSpec
                            .builderFor(schema)
                            .identity("event_date")
                            .build();


            catalog.createTable(
                    tableId,
                    schema,
                    partitionSpec
            );
        }


        TableLoader tableLoader =
                TableLoader.fromCatalog(
                        catalogLoader,
                        tableId
                );


        // =====================================================
        // 8. AGGREGATED METRIC -> ROWDATA
        // =====================================================

        DataStream<RowData> rowDataStream =
                aggregated
                        .map(new ToRowDataMapper())
                        .name("to-iceberg-rowdata");


        // =====================================================
        // 9. ICEBERG SINK
        // =====================================================

        FlinkSink
                .forRowData(rowDataStream)
                .tableLoader(tableLoader)
                .append();


        // =====================================================
        // 10. EXECUTE
        // =====================================================

        env.execute(
                "urban-lakehouse-streaming-java"
        );
    }


    // =========================================================
    // SENSOR EVENT
    // =========================================================

    public static class SensorEvent implements Serializable {

        public String sensorId;

        public double temperature;

        public double humidity;

        public int airQualityIndex;

        public long eventTimeMillis;


        public SensorEvent() {
        }


        public SensorEvent(
                String sensorId,
                double temperature,
                double humidity,
                int airQualityIndex,
                long eventTimeMillis
        ) {

            this.sensorId = sensorId;
            this.temperature = temperature;
            this.humidity = humidity;
            this.airQualityIndex = airQualityIndex;
            this.eventTimeMillis = eventTimeMillis;
        }
    }


    // =========================================================
    // JSON PARSER
    // =========================================================

    public static class SensorEventParser
            implements MapFunction<String, SensorEvent> {

        private static final ObjectMapper OBJECT_MAPPER =
                new ObjectMapper();


        private static final DateTimeFormatter SQL_TIMESTAMP =
                DateTimeFormatter.ofPattern(
                        "yyyy-MM-dd HH:mm:ss"
                );


        private static final DateTimeFormatter SQL_TIMESTAMP_MILLIS =
                DateTimeFormatter.ofPattern(
                        "yyyy-MM-dd HH:mm:ss.SSS"
                );


        @Override
        public SensorEvent map(String json) throws Exception {

            JsonNode root =
                    OBJECT_MAPPER.readTree(json);


            JsonNode sensorIdNode =
                    root.get("sensor_id");

            JsonNode temperatureNode =
                    root.get("temperature");

            JsonNode humidityNode =
                    root.get("humidity");

            JsonNode aqiNode =
                    root.get("air_quality_index");


            /*
             * Compatibilidad:
             *
             * PyFlink reciente -> event_time
             * productor historico -> timestamp
             */
            JsonNode timestampNode =
                    root.get("event_time");

            if (timestampNode == null
                    || timestampNode.isNull()) {

                timestampNode =
                        root.get("timestamp");
            }


            if (sensorIdNode == null
                    || temperatureNode == null
                    || humidityNode == null
                    || aqiNode == null
                    || timestampNode == null) {

                throw new IllegalArgumentException(
                        "Evento JSON incompleto: " + json
                );
            }


            long eventTimeMillis =
                    parseEventTimestamp(timestampNode);


            return new SensorEvent(

                    sensorIdNode.asText(),

                    temperatureNode.asDouble(),

                    humidityNode.asDouble(),

                    aqiNode.asInt(),

                    eventTimeMillis
            );
        }


        private static long parseEventTimestamp(
                JsonNode timestampNode
        ) {

            if (timestampNode.isNumber()) {
                return timestampNode.asLong();
            }


            String timestamp =
                    timestampNode.asText().trim();


            // ISO-8601 UTC:
            // 2026-09-05T15:20:00Z

            try {

                return Instant
                        .parse(timestamp)
                        .toEpochMilli();

            } catch (Exception ignored) {
            }


            // ISO con offset:
            // 2026-09-05T15:20:00-03:00

            try {

                return OffsetDateTime
                        .parse(timestamp)
                        .toInstant()
                        .toEpochMilli();

            } catch (Exception ignored) {
            }


            // SQL con milisegundos

            try {

                LocalDateTime dateTime =
                        LocalDateTime.parse(
                                timestamp,
                                SQL_TIMESTAMP_MILLIS
                        );

                return dateTime
                        .toInstant(ZoneOffset.UTC)
                        .toEpochMilli();

            } catch (Exception ignored) {
            }


            // SQL tradicional:
            // 2026-09-05 15:20:00

            try {

                LocalDateTime dateTime =
                        LocalDateTime.parse(
                                timestamp,
                                SQL_TIMESTAMP
                        );

                return dateTime
                        .toInstant(ZoneOffset.UTC)
                        .toEpochMilli();

            } catch (Exception e) {

                throw new IllegalArgumentException(
                        "Timestamp no reconocido: "
                                + timestamp,
                        e
                );
            }
        }
    }


    // =========================================================
    // ACCUMULATOR
    // =========================================================

    public static class MetricsAccumulator
            implements Serializable {

        public String sensorId;

        public double temperatureSum = 0.0;

        public double airQualitySum = 0.0;

        public long count = 0L;
    }


    // =========================================================
    // AGGREGATION
    // =========================================================

    public static class SensorAggregateFunction
            implements AggregateFunction<
                    SensorEvent,
                    MetricsAccumulator,
                    MetricsAccumulator> {


        @Override
        public MetricsAccumulator createAccumulator() {

            return new MetricsAccumulator();
        }


        @Override
        public MetricsAccumulator add(
                SensorEvent event,
                MetricsAccumulator accumulator
        ) {

            accumulator.sensorId =
                    event.sensorId;

            accumulator.temperatureSum +=
                    event.temperature;

            accumulator.airQualitySum +=
                    event.airQualityIndex;

            accumulator.count++;

            return accumulator;
        }


        @Override
        public MetricsAccumulator getResult(
                MetricsAccumulator accumulator
        ) {

            return accumulator;
        }


        @Override
        public MetricsAccumulator merge(
                MetricsAccumulator a,
                MetricsAccumulator b
        ) {

            MetricsAccumulator merged =
                    new MetricsAccumulator();

            merged.sensorId =
                    a.sensorId != null
                            ? a.sensorId
                            : b.sensorId;

            merged.temperatureSum =
                    a.temperatureSum
                            + b.temperatureSum;

            merged.airQualitySum =
                    a.airQualitySum
                            + b.airQualitySum;

            merged.count =
                    a.count
                            + b.count;

            return merged;
        }
    }


    // =========================================================
    // WINDOW INFORMATION
    // =========================================================

    public static class AddWindowInformation
            extends ProcessWindowFunction<
                    MetricsAccumulator,
                    AggregatedMetric,
                    String,
                    TimeWindow> {


        @Override
        public void process(
                String sensorId,
                Context context,
                Iterable<MetricsAccumulator> elements,
                Collector<AggregatedMetric> out
        ) {

            MetricsAccumulator accumulator =
                    elements.iterator().next();


            if (accumulator.count == 0) {
                return;
            }


            long windowStart =
                    context.window().getStart();

            long windowEnd =
                    context.window().getEnd();


            LocalDate eventDate =
                    Instant
                            .ofEpochMilli(windowStart)
                            .atZone(ZoneOffset.UTC)
                            .toLocalDate();


            AggregatedMetric metric =
                    new AggregatedMetric();

            metric.sensorId =
                    sensorId;

            metric.windowStart =
                    windowStart;

            metric.windowEnd =
                    windowEnd;

            metric.avgTemperature =
                    accumulator.temperatureSum
                            / accumulator.count;

            metric.avgAirQualityIndex =
                    accumulator.airQualitySum
                            / accumulator.count;

            metric.eventCount =
                    accumulator.count;

            metric.eventDate =
                    eventDate;


            out.collect(metric);
        }
    }


    // =========================================================
    // AGGREGATED RESULT
    // =========================================================

    public static class AggregatedMetric
            implements Serializable {

        public String sensorId;

        public long windowStart;

        public long windowEnd;

        public double avgTemperature;

        public double avgAirQualityIndex;

        public long eventCount;

        public LocalDate eventDate;
    }


    // =========================================================
    // ICEBERG ROWDATA
    // =========================================================

    public static class ToRowDataMapper
            implements MapFunction<AggregatedMetric, RowData> {


        @Override
        public RowData map(
                AggregatedMetric metric
        ) {

            GenericRowData row =
                    new GenericRowData(7);


            row.setField(
                    0,
                    StringData.fromString(
                            metric.sensorId
                    )
            );


            row.setField(
                    1,
                    TimestampData.fromEpochMillis(
                            metric.windowStart
                    )
            );


            row.setField(
                    2,
                    TimestampData.fromEpochMillis(
                            metric.windowEnd
                    )
            );


            row.setField(
                    3,
                    metric.avgTemperature
            );


            row.setField(
                    4,
                    metric.avgAirQualityIndex
            );


            row.setField(
                    5,
                    metric.eventCount
            );


            /*
             * Flink representa DATE internamente como
             * cantidad de dias desde 1970-01-01.
             */
            row.setField(
                    6,
                    (int) metric.eventDate.toEpochDay()
            );


            return row;
        }
    }
}