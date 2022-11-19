import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;

@Test(singleThreaded = true)
public class HudiSinkTest {

    private static final Logger logger =
            LoggerFactory.getLogger(HudiSinkTest.class.getName());

    private String createPartitionPath(final GenericRecord user1) {
        //todo: Remove leading "/" to see normal upsert behavior.
        return "/"+user1.get("favorite_color").toString()+"/"+user1.get("name");
    }
    @Test
    void HudiTest() throws IOException{

        Configuration hadoopConf = new Configuration();
        final String tablePath = System.getProperty("user.dir")+"/tmp/hudiTest/" ;
        final String tableType = HoodieTableType.COPY_ON_WRITE.name();
//        final String tableType = HoodieTableType.MERGE_ON_READ.name();
        final String tableName = "hudiTestTable";


        Path path = new Path(tablePath);
        FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
        if (!fs.exists(path)) {
            HoodieTableMetaClient.withPropertyBuilder()
                    .setTableType(HoodieTableType.valueOf(tableType))
                    .setTableName(tableName)
                    .setRecordKeyFields("ph")
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .setPreCombineField("entryTs")
                    .setBaseFileFormat("PARQUET")
                    .setPartitionFields("favorite_color,name")
                    .setHiveStylePartitioningEnable(true)
                    .setUrlEncodePartitioning(true)
                    .setCommitTimezone(HoodieTimelineTimeZone.UTC)
                    .setTableType(tableType)
                    .setTableName(tableName)
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .initTable(hadoopConf, tablePath);
        }


        final Schema schema = SchemaBuilder
                .record("user").doc("user").namespace("example.avro")
                .fields()
                .name("name").doc("Nome").type().stringType().noDefault()
                .name("favorite_number").doc("number").type().nullable().intType().noDefault()
                .name("favorite_color").doc("color").type().stringType().noDefault()
                .name("ph").doc("mobile").type().intType().noDefault()
                .name("entryTs").doc("tiebreaker on duplicates").type().longType().noDefault()
                .endRecord();


        final GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);
        user1.put("favorite_color","blue");
        user1.put("ph",2134567890);
        user1.put("entryTs",3);

        final GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");
        user2.put("ph",1234567890);
        user2.put("entryTs",1);

        String recordKey = user1.get("ph").toString();
        HoodieAvroPayload record = new HoodieAvroPayload(user1, 0);
        final HoodieAvroRecord<HoodieAvroPayload> hoodieAvroPayloadHoodieAvroRecord
                = new HoodieAvroRecord<>(new HoodieKey(recordKey, createPartitionPath(user1)), record);

        final HoodieAvroRecord<HoodieAvroPayload> hoodieAvroPayloadHoodieAvroRecord2
                = new HoodieAvroRecord<>(new HoodieKey(recordKey, createPartitionPath(user1)), record);




        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
                .withPath(tablePath)
                .withSchema(schema.toString())
                .combineInput(true,true) // testing
                .withPreCombineField("entryTs")           // testing
                .withParallelism(2, 2)
                .withDeleteParallelism(2).forTable(tableName)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
                .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build())
                .build();

        HoodieJavaWriteClient<HoodieAvroPayload> hudiClient = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(new Configuration()), cfg);

        String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        hudiClient.startCommitWithTime(newCommitTime);

        hudiClient.upsert(Collections.singletonList(hoodieAvroPayloadHoodieAvroRecord),newCommitTime);

        String newCommitTime2 = HoodieActiveTimeline.createNewInstantTime();
        hudiClient.startCommitWithTime(newCommitTime2);

        hudiClient.upsert(Collections.singletonList(hoodieAvroPayloadHoodieAvroRecord2),newCommitTime2);

        hudiClient.close();
    }


}
