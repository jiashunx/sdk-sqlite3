package io.github.jiashunx.sdk.sqlite3.mapping;

import io.github.jiashunx.sdk.sqlite3.mapping.service.SQLite3Service;
import io.github.jiashunx.sdk.sqlite3.mapping.util.SQLite3SQLHelper;
import io.github.jiashunx.sdk.sqlite3.metadata.annotation.SQLite3Column;
import io.github.jiashunx.sdk.sqlite3.metadata.annotation.SQLite3Id;
import io.github.jiashunx.sdk.sqlite3.metadata.annotation.SQLite3Table;
import io.github.jiashunx.sdk.sqlite3.metadata.xml.SQLPackage;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * SQLite3JdbcTemplate单元测试（模型映射）
 * @author jiashunx
 */
public class SQLite3JdbcTemplateTest4 {

    private SQLite3JdbcTemplate jdbcTemplate;

    @Before
    public void setUp() {
        SQLPackage sqlPackage = SQLite3SQLHelper.loadSQLPackageFromClasspath("test4.xml");
        jdbcTemplate = new SQLite3JdbcTemplate("/app/test/sdk-sqlite3/test4.db");
        jdbcTemplate.initSQLPackage(sqlPackage);
        assertNotNull(sqlPackage.getTableDefineSQL("aaa"));
        assertNotNull(sqlPackage.getViewDefineSQL("view_aaa"));
        assertNotNull(sqlPackage.getIndexDefineSQL("aaa", "index_aaa"));
        assertNotNull(sqlPackage.getTriggerDefineSQL("company_trigger"));
    }

    @Test
    public void test_trigger() {
        // sqlite没有truncate命令, 使用delete删除表记录
        jdbcTemplate.executeUpdate("delete from audit");
        jdbcTemplate.executeUpdate("delete from company");
        jdbcTemplate.executeUpdate("insert into company(id,name,age,address,salary) values(?,?,?,?,?)", statement -> {
            statement.setInt(1, 1);
            statement.setString(2, "Paul");
            statement.setInt(3, 23);
            statement.setString(4, "California");
            statement.setFloat(5, 20000.01F);
        });
        // 数据写入company后由触发器写入数据至audit
        assertEquals(1, jdbcTemplate.queryTableRowCount("audit"));
        assertEquals(1, jdbcTemplate.queryTableRowCount("company"));
    }

    @Test
    public void test_mapping() {
        AAAService service = new AAAService(jdbcTemplate, false);
        String id0 = "test-id";
        AAA entity0 = new AAA();
        entity0.setFieldId(id0);
        entity0.setFieldVarchar("varchar-value");
        entity0.setFieldNVarchar("nvarchar-value");
        entity0.setFieldInteger(100001);
        entity0.setFieldMediumInt(10002);
        entity0.setFieldInt(10003);
        entity0.setFieldInt4(10004);
        entity0.setFieldText("this is a text message");
        entity0.setFieldFloat(100.323F);
        entity0.setFieldBlob("fuck".getBytes(StandardCharsets.UTF_8));
        entity0.setFieldBoolean(true);
        entity0.setFieldBit(false);
        entity0.setFieldNumeric(new BigDecimal("1000.02"));
        entity0.setFieldDecimal(new BigDecimal("2001.03"));
        entity0.setFieldDate(new Date());
        entity0.setFieldTime(new Date());
        entity0.setFieldTimestamp(new Date());
        entity0.setFieldInt1((byte) 7);
        entity0.setFieldTinyint((byte) 6);
        entity0.setFieldInt2((short) 127);
        entity0.setFieldSmallint((short) 126);
        entity0.setFieldInt8(888888888888L);
        entity0.setFieldBigint(99999999999999999L);
        entity0.setFieldReal(9.0988D);
        entity0.setFieldDouble(8.0991D);
        entity0.setFieldChar("A");
        entity0.setFieldLongvarchar("B");
        entity0.setFieldClob("C");
        entity0.setFieldTinytext("D");
        entity0.setFieldMediumtext("E");
        entity0.setFieldLongtext("F");
        entity0.setFieldNchar("G");
        entity0.setFieldLongnvarchar("I");
        entity0.setFieldNclob("J");
        entity0.setFieldBinary("fuckX".getBytes(StandardCharsets.UTF_8));
        entity0.setFieldVarbinary("fuckY".getBytes(StandardCharsets.UTF_8));
        entity0.setFieldLongvarbinary("fuckZ".getBytes(StandardCharsets.UTF_8));

        service.deleteById(id0);
        service.insert(entity0);
        service.deleteById(id0);
        service.insert(entity0);
        service.update(entity0);

        AAA entity1 = service.find(id0);
        assertNotNull(entity1);
        assertEquals(entity0.getFieldVarchar(), entity1.getFieldVarchar());
        assertEquals(entity0.getFieldNVarchar(), entity1.getFieldNVarchar());
        assertEquals(entity0.getFieldInteger(), entity1.getFieldInteger());
        assertEquals(entity0.getFieldMediumInt(), entity1.getFieldMediumInt());
        assertEquals(entity0.getFieldInt(), entity1.getFieldInt());
        assertEquals(entity0.getFieldInt4(), entity1.getFieldInt4());
        assertEquals(entity0.getFieldText(), entity1.getFieldText());
        assertEquals(BigDecimal.valueOf(entity0.getFieldFloat()), BigDecimal.valueOf(entity1.getFieldFloat()));
        assertEquals("fuck", new String(entity1.getFieldBlob(), StandardCharsets.UTF_8));
        assertEquals(entity0.isFieldBoolean(), entity1.isFieldBoolean());
        assertEquals(entity0.isFieldBit(), entity1.isFieldBit());
        assertEquals(entity0.getFieldNumeric(), entity1.getFieldNumeric());
        assertEquals(entity0.getFieldDecimal(), entity1.getFieldDecimal());
        assertEquals(entity0.getFieldDate().getTime(), entity1.getFieldDate().getTime());
        assertEquals(entity0.getFieldTime().getTime(), entity1.getFieldTime().getTime());
        assertEquals(entity0.getFieldTimestamp().getTime(), entity1.getFieldTimestamp().getTime());
        assertEquals(entity0.getFieldInt1(), entity1.getFieldInt1());
        assertEquals(entity0.getFieldTinyint(), entity1.getFieldTinyint());
        assertEquals(entity0.getFieldInt2(), entity1.getFieldInt2());
        assertEquals(entity0.getFieldSmallint(), entity1.getFieldSmallint());
        assertEquals(entity0.getFieldInt8(), entity1.getFieldInt8());
        assertEquals(entity0.getFieldBigint(), entity1.getFieldBigint());
        assertEquals(BigDecimal.valueOf(entity0.getFieldReal()), BigDecimal.valueOf(entity1.getFieldReal()));
        assertEquals(BigDecimal.valueOf(entity0.getFieldDouble()), BigDecimal.valueOf(entity1.getFieldDouble()));
        assertEquals(entity0.getFieldChar(), entity1.getFieldChar());
        assertEquals(entity0.getFieldLongvarchar(), entity1.getFieldLongvarchar());
        assertEquals(entity0.getFieldClob(), entity1.getFieldClob());
        assertEquals(entity0.getFieldTinytext(), entity1.getFieldTinytext());
        assertEquals(entity0.getFieldMediumtext(), entity1.getFieldMediumtext());
        assertEquals(entity0.getFieldLongtext(), entity1.getFieldLongtext());
        assertEquals(entity0.getFieldNchar(), entity1.getFieldNchar());
        assertEquals(entity0.getFieldLongnvarchar(), entity1.getFieldLongnvarchar());
        assertEquals(entity0.getFieldNclob(), entity1.getFieldNclob());
        assertEquals("fuckX", new String(entity1.getFieldBinary(), StandardCharsets.UTF_8));
        assertEquals("fuckY", new String(entity1.getFieldVarbinary(), StandardCharsets.UTF_8));
        assertEquals("fuckZ", new String(entity1.getFieldLongvarbinary(), StandardCharsets.UTF_8));

        List<AAA> aaaList1 = service.selectWithPage(1, 10);
        assertTrue(aaaList1.size() > 0);
        List<AAA> aaaList2 = service.selectFieldsWithPage(List.of("field_id", "field_varchar"), 1, 10);
        assertTrue(aaaList2.size() > 0);
    }

    public static class AAAService extends SQLite3Service<AAA, String> {

        public AAAService(SQLite3JdbcTemplate jdbcTemplate, boolean cacheEnabled) {
            super(jdbcTemplate, cacheEnabled);
        }

        @Override
        protected Class<AAA> getEntityClass() {
            return AAA.class;
        }
    }

    @SQLite3Table(tableName = "aaa")
    public static class AAA implements Serializable {

        @SQLite3Id
        @SQLite3Column(columnName = "field_id")
        private String fieldId;
        @SQLite3Column(columnName = "field_varchar")
        private String fieldVarchar;
        @SQLite3Column(columnName = "field_nvarchar")
        private String fieldNVarchar;
        @SQLite3Column(columnName = "field_integer")
        private int fieldInteger;
        @SQLite3Column(columnName = "field_mediumint")
        private int fieldMediumInt;
        @SQLite3Column(columnName = "field_int")
        private int fieldInt;
        @SQLite3Column(columnName = "field_int4")
        private int fieldInt4;
        @SQLite3Column(columnName = "field_text")
        private String fieldText;
        @SQLite3Column(columnName = "field_float")
        private float fieldFloat;
        @SQLite3Column(columnName = "field_blob")
        private byte[] fieldBlob;
        @SQLite3Column(columnName = "field_boolean")
        private boolean fieldBoolean;
        @SQLite3Column(columnName = "field_bit")
        private boolean fieldBit;
        @SQLite3Column(columnName = "field_numeric")
        private BigDecimal fieldNumeric;
        @SQLite3Column(columnName = "field_decimal")
        private BigDecimal fieldDecimal;
        @SQLite3Column(columnName = "field_date")
        private Date fieldDate;
        @SQLite3Column(columnName = "field_time")
        private Date fieldTime;
        @SQLite3Column(columnName = "field_timestamp")
        private Date fieldTimestamp;
        @SQLite3Column(columnName = "field_int1")
        private byte fieldInt1;
        @SQLite3Column(columnName = "field_tinyint")
        private byte fieldTinyint;
        @SQLite3Column(columnName = "field_int2")
        private short fieldInt2;
        @SQLite3Column(columnName = "field_smallint")
        private short fieldSmallint;
        @SQLite3Column(columnName = "field_int8")
        private long fieldInt8;
        @SQLite3Column(columnName = "field_bigint")
        private long fieldBigint;
        @SQLite3Column(columnName = "field_real")
        private double fieldReal;
        @SQLite3Column(columnName = "field_double")
        private double fieldDouble;
        @SQLite3Column(columnName = "field_char")
        private String fieldChar;
        @SQLite3Column(columnName = "field_longvarchar")
        private String fieldLongvarchar;
        @SQLite3Column(columnName = "field_clob")
        private String fieldClob;
        @SQLite3Column(columnName = "field_tinytext")
        private String fieldTinytext;
        @SQLite3Column(columnName = "field_mediumtext")
        private String fieldMediumtext;
        @SQLite3Column(columnName = "field_longtext")
        private String fieldLongtext;
        @SQLite3Column(columnName = "field_nchar")
        private String fieldNchar;
        @SQLite3Column(columnName = "field_longnvarchar")
        private String fieldLongnvarchar;
        @SQLite3Column(columnName = "field_nclob")
        private String fieldNclob;
        @SQLite3Column(columnName = "field_binary")
        private byte[] fieldBinary;
        @SQLite3Column(columnName = "field_varbinary")
        private byte[] fieldVarbinary;
        @SQLite3Column(columnName = "field_longvarbinary")
        private byte[] fieldLongvarbinary;

        public String getFieldId() {
            return fieldId;
        }

        public void setFieldId(String fieldId) {
            this.fieldId = fieldId;
        }

        public String getFieldVarchar() {
            return fieldVarchar;
        }

        public void setFieldVarchar(String fieldVarchar) {
            this.fieldVarchar = fieldVarchar;
        }

        public String getFieldNVarchar() {
            return fieldNVarchar;
        }

        public void setFieldNVarchar(String fieldNVarchar) {
            this.fieldNVarchar = fieldNVarchar;
        }

        public int getFieldInteger() {
            return fieldInteger;
        }

        public void setFieldInteger(int fieldInteger) {
            this.fieldInteger = fieldInteger;
        }

        public int getFieldMediumInt() {
            return fieldMediumInt;
        }

        public void setFieldMediumInt(int fieldMediumInt) {
            this.fieldMediumInt = fieldMediumInt;
        }

        public int getFieldInt() {
            return fieldInt;
        }

        public void setFieldInt(int fieldInt) {
            this.fieldInt = fieldInt;
        }

        public int getFieldInt4() {
            return fieldInt4;
        }

        public void setFieldInt4(int fieldInt4) {
            this.fieldInt4 = fieldInt4;
        }

        public String getFieldText() {
            return fieldText;
        }

        public void setFieldText(String fieldText) {
            this.fieldText = fieldText;
        }

        public float getFieldFloat() {
            return fieldFloat;
        }

        public void setFieldFloat(float fieldFloat) {
            this.fieldFloat = fieldFloat;
        }

        public byte[] getFieldBlob() {
            return fieldBlob;
        }

        public void setFieldBlob(byte[] fieldBlob) {
            this.fieldBlob = fieldBlob;
        }

        public boolean isFieldBoolean() {
            return fieldBoolean;
        }

        public void setFieldBoolean(boolean fieldBoolean) {
            this.fieldBoolean = fieldBoolean;
        }

        public boolean isFieldBit() {
            return fieldBit;
        }

        public void setFieldBit(boolean fieldBit) {
            this.fieldBit = fieldBit;
        }

        public BigDecimal getFieldNumeric() {
            return fieldNumeric;
        }

        public void setFieldNumeric(BigDecimal fieldNumeric) {
            this.fieldNumeric = fieldNumeric;
        }

        public BigDecimal getFieldDecimal() {
            return fieldDecimal;
        }

        public void setFieldDecimal(BigDecimal fieldDecimal) {
            this.fieldDecimal = fieldDecimal;
        }

        public Date getFieldDate() {
            return fieldDate;
        }

        public void setFieldDate(Date fieldDate) {
            this.fieldDate = fieldDate;
        }

        public Date getFieldTime() {
            return fieldTime;
        }

        public void setFieldTime(Date fieldTime) {
            this.fieldTime = fieldTime;
        }

        public Date getFieldTimestamp() {
            return fieldTimestamp;
        }

        public void setFieldTimestamp(Date fieldTimestamp) {
            this.fieldTimestamp = fieldTimestamp;
        }

        public byte getFieldInt1() {
            return fieldInt1;
        }

        public void setFieldInt1(byte fieldInt1) {
            this.fieldInt1 = fieldInt1;
        }

        public byte getFieldTinyint() {
            return fieldTinyint;
        }

        public void setFieldTinyint(byte fieldTinyint) {
            this.fieldTinyint = fieldTinyint;
        }

        public short getFieldInt2() {
            return fieldInt2;
        }

        public void setFieldInt2(short fieldInt2) {
            this.fieldInt2 = fieldInt2;
        }

        public short getFieldSmallint() {
            return fieldSmallint;
        }

        public void setFieldSmallint(short fieldSmallint) {
            this.fieldSmallint = fieldSmallint;
        }

        public long getFieldInt8() {
            return fieldInt8;
        }

        public void setFieldInt8(long fieldInt8) {
            this.fieldInt8 = fieldInt8;
        }

        public long getFieldBigint() {
            return fieldBigint;
        }

        public void setFieldBigint(long fieldBigint) {
            this.fieldBigint = fieldBigint;
        }

        public double getFieldReal() {
            return fieldReal;
        }

        public void setFieldReal(double fieldReal) {
            this.fieldReal = fieldReal;
        }

        public double getFieldDouble() {
            return fieldDouble;
        }

        public void setFieldDouble(double fieldDouble) {
            this.fieldDouble = fieldDouble;
        }

        public String getFieldChar() {
            return fieldChar;
        }

        public void setFieldChar(String fieldChar) {
            this.fieldChar = fieldChar;
        }

        public String getFieldLongvarchar() {
            return fieldLongvarchar;
        }

        public void setFieldLongvarchar(String fieldLongvarchar) {
            this.fieldLongvarchar = fieldLongvarchar;
        }

        public String getFieldClob() {
            return fieldClob;
        }

        public void setFieldClob(String fieldClob) {
            this.fieldClob = fieldClob;
        }

        public String getFieldTinytext() {
            return fieldTinytext;
        }

        public void setFieldTinytext(String fieldTinytext) {
            this.fieldTinytext = fieldTinytext;
        }

        public String getFieldMediumtext() {
            return fieldMediumtext;
        }

        public void setFieldMediumtext(String fieldMediumtext) {
            this.fieldMediumtext = fieldMediumtext;
        }

        public String getFieldLongtext() {
            return fieldLongtext;
        }

        public void setFieldLongtext(String fieldLongtext) {
            this.fieldLongtext = fieldLongtext;
        }

        public String getFieldNchar() {
            return fieldNchar;
        }

        public void setFieldNchar(String fieldNchar) {
            this.fieldNchar = fieldNchar;
        }

        public String getFieldLongnvarchar() {
            return fieldLongnvarchar;
        }

        public void setFieldLongnvarchar(String fieldLongnvarchar) {
            this.fieldLongnvarchar = fieldLongnvarchar;
        }

        public String getFieldNclob() {
            return fieldNclob;
        }

        public void setFieldNclob(String fieldNclob) {
            this.fieldNclob = fieldNclob;
        }

        public byte[] getFieldBinary() {
            return fieldBinary;
        }

        public void setFieldBinary(byte[] fieldBinary) {
            this.fieldBinary = fieldBinary;
        }

        public byte[] getFieldVarbinary() {
            return fieldVarbinary;
        }

        public void setFieldVarbinary(byte[] fieldVarbinary) {
            this.fieldVarbinary = fieldVarbinary;
        }

        public byte[] getFieldLongvarbinary() {
            return fieldLongvarbinary;
        }

        public void setFieldLongvarbinary(byte[] fieldLongvarbinary) {
            this.fieldLongvarbinary = fieldLongvarbinary;
        }
    }

}
