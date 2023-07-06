
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;


class Operation {
    private String op;
    private String ts_ms;
    private Source source;
    private Record before;
    private String before_field_type;
    private Record after;
    private String after_field_type;

    public Operation(String op, String ts_ms, Source source, Record before,
                     String before_field_type, Record after, String after_field_type) {
        this.op = op;
        this.ts_ms = ts_ms;
        this.source = source;
        this.before = before;
        this.before_field_type = before_field_type;
        this.after = after;
        this.after_field_type = after_field_type;

    }

    public Operation(String op, String ts_ms, Source source, Record after, String after_field_type) {
        this.op = op;
        this.ts_ms = ts_ms;
        this.source = source;
        this.before = null;  // 设置before为null
        this.before_field_type = null;  // 设置before_field_type为null
        this.after = after;
        this.after_field_type = after_field_type;
    }
    // Getter and setter methods

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getTs_ms() {
        return ts_ms;
    }

    public void setTs_ms(String ts_ms) {
        this.ts_ms = ts_ms;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Record getBefore() {
        return before;
    }

    public void setBefore(Record before) {
        this.before = before;
    }

    public String getBefore_field_type() {
        return before_field_type;
    }

    public void setBefore_field_type(String before_field_type) {
        this.before_field_type = before_field_type;
    }

    public Record getAfter() {
        return after;
    }

    public void setAfter(Record after) {
        this.after = after;
    }

    public String getAfter_field_type() {
        return after_field_type;
    }

    public void setAfter_field_type(String after_field_type) {
        this.after_field_type = after_field_type;
    }
}

class Source {
    private String ts_ms;
    private String db;
    private String schema;
    private String table;

    public Source(String ts_ms, String db, String schema, String table) {
        this.ts_ms = ts_ms;
        this.db = db;
        this.schema = schema;
        this.table = table;
    }

    // Getter and setter methods

    public String getTs_ms() {
        return ts_ms;
    }

    public void setTs_ms(String ts_ms) {
        this.ts_ms = ts_ms;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}

/**
 boolean
 bigint
 int
 tinyint
 smallint
 integer
 mediumint
 double
 float

 date
 datetime
 timestamp
 decimal
 char
 varchar
 string
 longtext
 mediumtext
 text
 tinytext
 json
 */
class Record {
    private int id;  // Assuming
    //boolean
    private boolean col1;
    //bigint
    private long col2;
    //int
    private int col3;
    private int col4;
    private int col5;
    private int col6;
    private int col7;
    //double
    private double col8;
    //float
    private float col9;
    //date
    private String col10;
    private String col11;
    private String col12;
    //decimal
    private String col13;
    //char
    private String col14;
    //string
    private String col15;
    private String col16;
    private String col17;
    private String col18;
    private String col19;
    private String col20;
    private String col21;

    public Record(int id, boolean col1, long col2, int col3, int col4, int col5, int col6, int col7, double col8, float col9, String col10, String col11, String col12, String col13, String col14, String col15, String col16, String col17, String col18, String col19, String col20, String col21) {
        this.id = id;
        this.col1 = col1;
        this.col2 = col2;
        this.col3 = col3;
        this.col4 = col4;
        this.col5 = col5;
        this.col6 = col6;
        this.col7 = col7;
        this.col8 = col8;
        this.col9 = col9;
        this.col10 = col10;
        this.col11 = col11;
        this.col12 = col12;
        this.col13 = col13;
        this.col14 = col14;
        this.col15 = col15;
        this.col16 = col16;
        this.col17 = col17;
        this.col18 = col18;
        this.col19 = col19;
        this.col20 = col20;
        this.col21 = col21;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public boolean isCol1() {
        return col1;
    }

    public void setCol1(boolean col1) {
        this.col1 = col1;
    }

    public long getCol2() {
        return col2;
    }

    public void setCol2(long col2) {
        this.col2 = col2;
    }

    public int getCol3() {
        return col3;
    }

    public void setCol3(int col3) {
        this.col3 = col3;
    }

    public int getCol4() {
        return col4;
    }

    public void setCol4(int col4) {
        this.col4 = col4;
    }

    public int getCol5() {
        return col5;
    }

    public void setCol5(int col5) {
        this.col5 = col5;
    }

    public int getCol6() {
        return col6;
    }

    public void setCol6(int col6) {
        this.col6 = col6;
    }

    public int getCol7() {
        return col7;
    }

    public void setCol7(int col7) {
        this.col7 = col7;
    }

    public double getCol8() {
        return col8;
    }

    public void setCol8(int col8) {
        this.col8 = col8;
    }

    public float getCol9() {
        return col9;
    }

    public void setCol9(int col9) {
        this.col9 = col9;
    }

    public String getCol10() {
        return col10;
    }

    public void setCol10(String col10) {
        this.col10 = col10;
    }

    public String getCol11() {
        return col11;
    }

    public void setCol11(String col11) {
        this.col11 = col11;
    }

    public String getCol12() {
        return col12;
    }

    public void setCol12(String col12) {
        this.col12 = col12;
    }

    public String getCol13() {
        return col13;
    }

    public void setCol13(String col13) {
        this.col13 = col13;
    }

    public String getCol14() {
        return col14;
    }

    public void setCol14(String col14) {
        this.col14 = col14;
    }

    public String getCol15() {
        return col15;
    }

    public void setCol15(String col15) {
        this.col15 = col15;
    }

    public String getCol16() {
        return col16;
    }

    public void setCol16(String col16) {
        this.col16 = col16;
    }

    public String getCol17() {
        return col17;
    }

    public void setCol17(String col17) {
        this.col17 = col17;
    }

    public String getCol18() {
        return col18;
    }

    public void setCol18(String col18) {
        this.col18 = col18;
    }

    public String getCol19() {
        return col19;
    }

    public void setCol19(String col19) {
        this.col19 = col19;
    }

    public String getCol20() {
        return col20;
    }

    public void setCol20(String col20) {
        this.col20 = col20;
    }

    public String getCol21() {
        return col21;
    }

    public void setCol21(String col21) {
        this.col21 = col21;
    }
}

class FieldTypeInfo {
    // Add necessary fields based on your requirement
    // Assuming the fields are of String type in this example
    private String id;
    private String col1;
    private String col2;
    private String col3;
    private String strCol4;
    private String dateCol5;
    private String dateTimeCol6;
    private String charCol7;
    private String floatClo8;
    private String timeStampCol9;

    // Getter and setter methods

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCol1() {
        return col1;
    }

    public void setCol1(String col1) {
        this.col1 = col1;
    }

    public String getCol2() {
        return col2;
    }

    public void setCol2(String col2) {
        this.col2 = col2;
    }

    public String getCol3() {
        return col3;
    }

    public void setCol3(String col3) {
        this.col3 = col3;
    }
}

class Wrapper {
    private PKValue pk_value;

    public Wrapper(PKValue pk_value) {
        this.pk_value = pk_value;
    }
}

class PKValue {
    private int id;

    public PKValue(int id) {
        this.id = id;
    }
}

class GenerateJson {

    public static String fieldType = "{id:INT precision:11," +
            "col1:boolean," +
            "col2:bigint," +
            "col3:INT," +
            "col4:tinyint," +
            "col5:smallint," +
            "col6:integer," +
            "col7:mediumint," +
            "col8:double," +
            "col9:float," +
            "col10:date," +
            "col11:datetime," +
            "col12:timestamp," +
            "col13:decimal precision:10 scale:2" +
            "col14:char," +
            "col15:varchar," +
            "col16:string," +
            "col17:longtext," +
            "col18:mediumtext," +
            "col19:text," +
            "col20:tinytext," +
            "col21:json" +
            "}";

    public static SimpleDateFormat df1 = new SimpleDateFormat("YYYY-MM-dd");
    public static SimpleDateFormat df2 = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
    public static Random random = new Random();
    public static DecimalFormat df3 = new DecimalFormat("######.###");


    public static void main(String[] args) {

        Gson gson = new GsonBuilder().serializeNulls().create();

        List<Operation> operations = generateOperations(1000);
        List<Operation> upOps = new ArrayList<>();
        List<Operation> deOps = new ArrayList<>();


        //装载最后含kv的json数据
        List<String> jsonList = new ArrayList<>();
        List<String> upOpsList = new ArrayList<>();
        List<String> deOpsList = new ArrayList<>();

        //从1~1000随机抽10条数据进行更新
        // 创建一个Random对象
        // 创建一个Set用于存储已选择的数字
        Set<Integer> selectedNumbers = new LinkedHashSet<>();
        // 生成10个不重复的随机整数
        while (selectedNumbers.size() < 10) {
            int randomNumber = random.nextInt(1000) + 1;
            selectedNumbers.add(randomNumber);
        }

        for (int j : selectedNumbers) {
            Operation upOperation = operations.get(j - 1);
            Operation newOp = new Operation("U", timeStamp, upOperation.getSource(), upOperation.getAfter(), upOperation.getAfter_field_type(),
                    new Record(j,genRandBoolean(),genRandLong(),genRandInt(),genRandomTinyint(),genrandomSmallInt(), genRandInt(),
                            genRandInt(),genrandomDouble(),genrandomFloat(),gendateCol(),gendatetime(),gendatetime(),gendecimalStr(),
                            genrandomChar(),genrandomString(),genrandomString(),genrandomString(),genrandomString(),
                            genrandomString(),genrandomString(),genjsonStr()), upOperation.getAfter_field_type());
            upOps.add(newOp);
            String s = gson.toJson(newOp);
            //System.out.println(s);
        }

        //从1~100随机抽10条不重复数据进行删除
        Set<Integer> toDeletNum = new LinkedHashSet<>();
        while (toDeletNum.size() < 10) {
            int randomNUmber = random.nextInt(1000) + 1;
            toDeletNum.add(randomNUmber);
        }
        for (int j : toDeletNum) {
            Operation deOperation = operations.get(j - 1);
            Operation newOp = new Operation("D", timeStamp, deOperation.getSource(), deOperation.getAfter(), deOperation.getAfter_field_type(),
                    null, null);
            deOps.add(newOp);
            String s = gson.toJson(newOp);
            //System.out.println(s);
        }
        //Convert operations to JSON

        int i = 0;
        int countUp = 0;
        int countDe = 0;

        for (Operation operation : upOps) {
            PKValue pkValue = new PKValue((Integer) selectedNumbers.toArray()[countUp++]);
            Wrapper wrapper = new Wrapper(pkValue);
            String pk_json = gson.toJson(wrapper);
            String s = gson.toJson(operation);
            upOpsList.add(pk_json);
            upOpsList.add(s);
        }

        for (Operation operation : deOps) {
            PKValue pkValue = new PKValue((Integer) toDeletNum.toArray()[countDe++]);
            Wrapper wrapper = new Wrapper(pkValue);
            String pk_json = gson.toJson(wrapper);
            String s = gson.toJson(operation);
            deOpsList.add(pk_json);
            deOpsList.add(s);
        }
        for (Operation operation : operations) {
            PKValue pkValue = new PKValue(operation.getAfter().getId());
            Wrapper wrapper = new Wrapper(pkValue);
            String json = gson.toJson(operation);
            //JsonObject pk = new JsonObject();
            String pk_json = gson.toJson(wrapper);
            // 将JSON对象转换为JSON字符串
            jsonList.add(pk_json);
            jsonList.add(json);
        }

//        for (int i1 = 0; i1 < upOpsList.size(); i1++) {
//            System.out.println(upOpsList.toArray()[i1]);
//        }
//        for (int i1 = 0; i1 < deOpsList.size(); i1++) {
//            System.out.println(deOpsList.toArray()[i1]);
//        }

//        for (int i1 = 0; i1 < jsonList.size(); i1++) {
//            System.out.println(jsonList.toArray()[i1]);
//        }


        send(topicName,operations);


    }

    private static String topicName = "t_nanhang_test";


    private static Properties getConf() {

        Properties properties = new Properties();
        // ProducerConfig 类定义了生产者相关的配置
        // 指定kafka 连接的集群
        properties.put("bootstrap.servers", "192.168.25.34:9093");
//        properties.put("bootstrap.servers", "localhost:9092");
        // ack应答级别
        properties.put("acks", "all");
        // 重试次数
        properties.put("retries", 0);
        // 单次发送批次的大小
        properties.put("batch.size", 163840);
//        properties.put("batch.size", 1);
        // 批次大小不足的情况等待发送时间
        properties.put("linger.ms", 1);
        // RecordAccumulator 缓冲区大小
        properties.put("buffer.memory", 33554432);
//        properties.put("buffer.memory", 2000);

        // key - value序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

    static void send(String topicName,List<Operation> operations) {

        Gson gson = new GsonBuilder().serializeNulls().create();
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(getConf());

//            List<Operation> operations = SourceGeneration.generateOperations(10);
            for (Operation o : operations) {
                PKValue pkValue = new PKValue(o.getAfter().getId());
                Wrapper wrapper = new Wrapper(pkValue);
                String value = gson.toJson(o);
                String key = gson.toJson(wrapper);
                System.out.println(key);
                System.out.println(value);
                System.out.println("====");
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
                producer.send(record);
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }finally {
            // 一定要关闭资源，否则消息发送不成功
            try {
                if (producer != null) {
                    producer.close();
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }

    static Long timeMillis = System.currentTimeMillis();
    static String timeStamp = timeMillis.toString();

    /**
     * 1.boolean bigint int tinyint smallint 6.integer mediumint double float date
     * 11.datetime timestamp decimal char varchar
     * 16.string longtext mediumtext text tinytext
     * 21.json
     * @param
     * @return
     */

    public static boolean genRandBoolean(){
        return random.nextBoolean();
    }
    public static long genRandLong(){
        return random.nextLong();
    }
    public static int genRandInt(){
        return random.nextInt(100);
    }
    public static int genRandomTinyint(){
        return random.nextInt(256) - 128;
    }
    public static int genrandomSmallInt(){
        return random.nextInt(65535) - 32768;
    }
    public static float genrandomFloat(){
        return random.nextFloat();
    }
    public static double genrandomDouble(){
        return random.nextDouble();
    }
    public static String genrandomString(){
        return RandomStringUtils.randomAlphanumeric(20);
    }
    public static String gendateCol(){
        return df1.format(new java.util.Date());
    }
    public static String gendatetime(){
        return df2.format(new java.util.Date());
    }
    public static String gendecimalStr(){
        float randomFloat = random.nextFloat();
        return df3.format(randomFloat);
    }
    public static String genrandomChar(){
        return RandomStringUtils.randomAlphabetic(1);
    }
    public static String genjsonStr(){
        return "{name:soul}";
    }
    public static List<Operation> generateOperations(int count) {

        List<Operation> operations = new ArrayList<>();
        List<Operation> operationsUp = new ArrayList<>();

        for (int i = 1; i <= count; i++) {
            boolean randomBoolean = genRandBoolean();
            long randomLong = random.nextLong();
            int randomInt = random.nextInt(100);
            int randomTinyint = random.nextInt(256) - 128;
            int randomSmallInt = random.nextInt(65535) - 32768;
            float randomFloat = random.nextFloat();
            double randomDouble = random.nextDouble();

            String randomString = RandomStringUtils.randomAlphanumeric(20);
            String dateCol = df1.format(new java.util.Date());
            String datetime = df2.format(new java.util.Date());
            String decimalStr = df3.format(randomFloat);
            String randomChar = RandomStringUtils.randomAlphabetic(1);
            String jsonStr = "{name:lake}";

            Record record = new Record(i, randomBoolean, randomLong, randomInt, randomTinyint, randomSmallInt,
                    randomInt, randomInt, randomDouble, randomFloat, dateCol,
                    datetime, datetime, decimalStr, randomChar, randomString, randomString, randomString, randomString, randomString,
                    randomString, jsonStr);

            // Create the source information
            Source source = new Source(timeStamp, "nanhang", "nanhang", "test");

            // Create the operation for insertion
            Operation insertionOperation = new Operation("I", timeStamp, source, record, fieldType);

            operations.add(insertionOperation);
        }
        return operations;
    }
}