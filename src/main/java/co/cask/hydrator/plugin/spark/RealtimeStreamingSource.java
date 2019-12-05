/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.spark;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Realtime CDAP spark streaming source
 */
//@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("Stream")
@Description("CDAP stream realtime spark streaming source.")
public class RealtimeStreamingSource extends StreamingSource<StructuredRecord> {

  private static final String FORMAT_SETTING_PREFIX = "format.setting.";
  private static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("body", Schema.of(Schema.Type.BYTES))
  );
  private static final String NAME = "name";
  private static final String NAME_DESCRIPTION = "Name of the stream. Must be a valid stream name. " +
    "If it doesn't exist, it will be created. " +
    "If the name of the stream is provided during runtime through a macro, it has to exist before.";
  private static final String FORMAT_DESCRIPTION = "Optional format of the stream. Any format supported by CDAP " +
    "is also supported. For example, a value of 'csv' will attempt to parse stream events as comma-separated values. " +
    "If no format is given, event bodies will be treated as bytes, resulting in a three-field schema: " +
    "'ts' of type long, 'headers' of type map of string to string, and 'body' of type bytes.";
  private static final String SCHEMA_DESCRIPTION = "Optional schema for the body of stream events. Schema is used " +
    "in conjunction with format to parse stream events. Some formats like the avro format require schema, " +
    "while others do not. The schema given is for the body of the stream, so the final schema of records output " +
    "by the source will contain an additional field named 'ts' for the timestamp and a field named 'headers' " +
    "for the headers as the first and second fields of the schema.";


  private final RealtimeStreamConfig config;

  public RealtimeStreamingSource(RealtimeStreamConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    if (!config.containsMacro(NAME)) {
      pipelineConfigurer.addStream(new Stream(config.name));
    }
    // if no format is specified then default schema is used, if otherwise its based on format spec.
    if (config.format == null) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(DEFAULT_SCHEMA);
    } else if (config.getFormatSpec() != null && config.getFormatSpec().getSchema() != null) {
      List<Schema.Field> fields = Lists.newArrayList();
      fields.add(Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
      fields.add(Schema.Field.of("headers",
                                 Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));
      fields.addAll(config.getFormatSpec().getSchema().getFields());
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.recordOf("event", fields));
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {
    JavaStreamingContext jsc = streamingContext.getSparkStreamingContext();
    Field f = streamingContext.getClass().getDeclaredField("sec");
    f.setAccessible(true);
    JavaSparkExecutionContext sec = (JavaSparkExecutionContext) f.get(streamingContext);
    return createStreamInputDStream(sec, jsc, sec.getNamespace(), config.name, config.getFormatSpec());

  }

  private JavaDStream<StructuredRecord> createStreamInputDStream(JavaSparkExecutionContext sec,
                                                                 JavaStreamingContext jsc,
                                                                 String namespace,
                                                                 String streamName,
                                                                 @Nullable FormatSpecs formatSpec) {

    if (formatSpec == null) {
      return JavaDStream.fromDStream(new StreamInputDStream(sec, jsc.ssc(), namespace, streamName),
                                     ClassTag$.MODULE$.<StreamEvent>apply(StreamEvent.class))
        .map(new Function<StreamEvent, StructuredRecord>() {
          @Override
          public StructuredRecord call(StreamEvent event) throws Exception {
            return StructuredRecord.builder(DEFAULT_SCHEMA)
              .set("ts", event.getTimestamp())
              .set("headers", event.getHeaders())
              .set("body", event.getBody())
              .build();
          }
        });
    }

    return JavaDStream.fromDStream(
      new StructuredRecordStreamInputDStream(sec, jsc.ssc(), namespace, streamName, formatSpec),
      ClassTag$.MODULE$.<StructuredRecord>apply(StructuredRecord.class));
  }

  /**
   * {@link ReferencePluginConfig} class for {@link RealtimeStreamingSource}
   */
  public static class RealtimeStreamConfig extends PluginConfig implements Serializable {


    @Name(NAME)
    @Description(NAME_DESCRIPTION)
    @Macro
    private String name;

    @Description(FORMAT_DESCRIPTION)
    @Nullable
    private String format;

    @Description(SCHEMA_DESCRIPTION)
    @Nullable
    private String schema;

    private void validate() {
      // check the schema if there is one
      if (!Strings.isNullOrEmpty(schema)) {
        parseSchema();
      }
    }

    private FormatSpecs getFormatSpec() {
      FormatSpecs formatSpec = null;
      if (!Strings.isNullOrEmpty(format)) {
        // try to parse the schema if there is one
        Schema schemaObj = parseSchema();

        // strip format.settings. from any properties and use them in the format spec
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : getProperties().getProperties().entrySet()) {
          if (entry.getKey().startsWith(FORMAT_SETTING_PREFIX)) {
            String key = entry.getKey();
            builder.put(key.substring(FORMAT_SETTING_PREFIX.length(), key.length()), entry.getValue());
          }
        }
        formatSpec = new FormatSpecs(format, schemaObj, builder.build());
      }
      return formatSpec;
    }

    private Schema parseSchema() {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
      }
    }
  }

}
